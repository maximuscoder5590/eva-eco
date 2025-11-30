from flask import Flask, request, jsonify
import requests, os, time, uuid, sys, traceback, logging, yaml
from logging.handlers import RotatingFileHandler
from functools import wraps

# --- logging setup (use root logger so messages go to stdout and file) ---
LOG_DIR = '/var/log/orchestrator'
try:
    os.makedirs(LOG_DIR, exist_ok=True)
except Exception:
    pass

logger = logging.getLogger()  # root logger
logger.setLevel(logging.DEBUG)
fmt = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')

# File handler
try:
    fh = RotatingFileHandler(os.path.join(LOG_DIR, 'orchestrator.log'), maxBytes=5_000_000, backupCount=3)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
except Exception:
    # if file handler cannot be created, keep going; root logger will still emit to stdout
    pass

# Stream handler (stdout)
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(fmt)
logger.addHandler(sh)

ERROR_FILE = os.path.join(LOG_DIR, 'orchestrator_error.log')

def handle_exception(exc_type, exc_value, exc_tb):
    tb = ''.join(traceback.format_exception(exc_type, exc_value, exc_tb))
    logger.error('UNCAUGHT EXCEPTION:\\n%s', tb)
    try:
        with open(ERROR_FILE, 'a', encoding='utf-8') as f:
            f.write(tb + '\\n')
    except Exception:
        logger.exception('Failed writing to error file')
sys.excepthook = handle_exception

# --- app & config ---
app = Flask(__name__)

AGENTS = [
    ('mdc', 'http://mdc:80/run'),
    ('mar', 'http://mar:80/run'),
    ('cfa', 'http://cfa:80/run'),
    ('cps', 'http://cps:80/run'),
    ('mbo', 'http://mbo:80/run'),
    ('ftm', 'http://ftm:80/run'),
]

# Load agent policy (optional)
POLICY_FILE = '/app/agent_policy.yml'
agent_policy = {}
try:
    if os.path.exists(POLICY_FILE):
        with open(POLICY_FILE, 'r', encoding='utf-8') as f:
            agent_policy = yaml.safe_load(f) or {}
        logger.info('Loaded agent policy from %s: %s', POLICY_FILE, agent_policy)
    else:
        logger.info('No agent_policy.yml found at %s, using defaults', POLICY_FILE)
except Exception:
    logger.exception('Failed loading agent policy, using defaults')

# retry decorator (network/errors)
def retry(tries=3, delay=1, backoff=2, allowed_exceptions=(Exception,)):
    def deco(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            _tries, _delay = tries, delay
            last_exc = None
            while _tries > 0:
                try:
                    return f(*args, **kwargs)
                except allowed_exceptions as e:
                    last_exc = e
                    logger.warning('Retryable error in %s: %s — tries left %s', f.__name__, e, _tries-1)
                    time.sleep(_delay)
                    _tries -= 1
                    _delay *= backoff
            raise last_exc
        return wrapper
    return deco

@retry(tries=3, delay=1)
def call_agent(name, url, job, payload, timeout=20):
    req = {
        'job': job,
        'input_timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'payload': payload,
        'request_id': str(uuid.uuid4())
    }
    logger.debug('Calling agent %s @ %s with job=%s payload_keys=%s', name, url, job, (list(payload.keys()) if isinstance(payload, dict) else type(payload)))
    try:
        r = requests.post(url, json=req, timeout=timeout)
        logger.info('Agent %s responded %s', name, r.status_code)
        try:
            body = r.json()
        except Exception:
            body = {'status':'error','meta':{'agent':name},'issues':[{'note':'invalid json from agent','raw':r.text}]}
        return r.status_code, body
    except Exception as e:
        logger.exception('Exception calling agent %s', name)
        return 0, {'status':'error','meta':{'agent':name,'job':job},'issues':[{'note':str(e)}]}

def attempt_partial_retry(name, url, job, payload, retries=2):
    logger.info('Attempting %s retries for partial from %s', retries, name)
    attempt = 0
    backoff = 1
    last_status_code = 0
    last_resp = None
    while attempt < retries:
        attempt += 1
        time.sleep(backoff)
        logger.debug('Partial retry %s for %s (backoff %s)', attempt, name, backoff)
        status_code, resp = call_agent(name, url, job, payload)
        last_status_code = status_code
        last_resp = resp
        if resp and resp.get('status') != 'partial':
            logger.info('Agent %s after retry returned status %s', name, resp.get('status'))
            return status_code, resp, attempt
        backoff *= 2
    logger.warning('Agent %s remains partial after %s retries', name, retries)
    return last_status_code, last_resp, attempt

@app.route('/run', methods=['POST'])
def run():
    try:
        payload = request.get_json(force=True, silent=True) or {}
        request_id = payload.get('request_id','local-'+str(uuid.uuid4()))
        date_from = payload.get('date_from')
        date_to = payload.get('date_to')
        campaign_ids = payload.get('campaign_ids',[])
        final_provenance = []
        aggregated_outputs = {}
        overall_partial = False

        next_payload = {'campaign_ids':campaign_ids, 'date_from':date_from, 'date_to':date_to}
        for name, url in AGENTS:
            status_code, resp = call_agent(name, url, 'job_from_eva', next_payload)
            if resp is None:
                resp = {'status':'error','meta':{'agent':name},'issues':[{'note':'no response'}]}

            p = agent_policy.get(name, {}) if isinstance(agent_policy, dict) else {}
            partial_retries = int(p.get('partial_retries', 2))
            partial_is_error = bool(p.get('partial_is_error', False))

            # handle partial with policy
            if resp.get('status') == 'partial' and partial_retries > 0:
                status_code, resp_after, attempts = attempt_partial_retry(name, url, 'job_from_eva', next_payload, retries=partial_retries)
                if resp_after:
                    resp = resp_after
                if resp.get('status') == 'partial':
                    resp.setdefault('issues', []).append({'note': 'partial_after_retries', 'attempts': attempts})

            if resp.get('status') == 'partial' and partial_is_error:
                logger.error('Agent %s returned partial and policy marks it as error; stopping', name)
                final_provenance.append({
                    'agent': name,
                    'status': 'error',
                    'meta': resp.get('meta'),
                    'issues': resp.get('issues', []) + [{'note': 'partial_treated_as_error_by_policy'}],
                    'data_sample': (resp.get('data') or [])[:1]
                })
                final_report = {
                    'request_id': request_id,
                    'status': 'error',
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                    'provenance': final_provenance,
                    'aggregated_outputs': aggregated_outputs,
                    'notes': f'Stopped due to agent {name} partial flagged as error by policy'
                }
                return jsonify(final_report), 500

            if resp.get('status') == 'partial':
                overall_partial = True

            final_provenance.append({
                'agent': name,
                'status': resp.get('status','error'),
                'meta': resp.get('meta'),
                'issues': resp.get('issues',[]),
                'data_sample': (resp.get('data') or [])[:1]
            })

            next_payload = {'input_data': resp.get('data',[])}

            if resp.get('status') == 'error':
                final_report = {
                    'request_id': request_id,
                    'status': 'error',
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                    'provenance': final_provenance,
                    'aggregated_outputs': aggregated_outputs,
                    'notes': 'Stopped due to agent error'
                }
                logger.error('Stopping run %s due to agent error at %s', request_id, name)
                return jsonify(final_report), 500

        aggregated_outputs['metrics_summary'] = {'note':'sample aggregated outputs'}
        final_status = 'partial' if overall_partial else 'ok'
        final_report = {
            'request_id': request_id,
            'status': final_status,
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            'provenance': final_provenance,
            'aggregated_outputs': aggregated_outputs
        }
        logger.info('Run %s completed %s', request_id, final_status)
        return jsonify(final_report), 200
    except Exception:
        logger.exception('Fatal exception in /run handler')
        return jsonify({'status':'error','note':'internal'}), 500

# alias endpoint /e2e to /run
@app.route('/e2e', methods=['POST'])
def e2e_alias():
    return run()

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status':'ok','time': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}), 200

if __name__ == '__main__':
    os.environ['PYTHONUNBUFFERED'] = '1'
    logger.info('Starting orchestrator app on 0.0.0.0:8080')
    app.run(host='0.0.0.0', port=8080)
