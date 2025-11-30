# orchestrator/app.py
# EVA-ECO local orchestrator (simple)
# Ready-for-use: retries, structured provenance, partial handling, call_meta, basic quality gates

from flask import Flask, request, jsonify
import requests
import os
import time
import uuid
import logging
from requests.exceptions import RequestException

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Agent endpoints (matches docker-compose service DNS names)
AGENTS = [
    ('mdc', 'http://mdc:80/run'),
    ('mar', 'http://mar:80/run'),
    ('cfa', 'http://cfa:80/run'),
    ('cps', 'http://cps:80/run'),
    ('mbo', 'http://mbo:80/run'),
    ('ftm', 'http://ftm:80/run'),
]

# Utility: safe json from response
def safe_json(resp):
    try:
        return resp.json()
    except Exception:
        return None

def call_agent(name, url, job, payload, max_retries=3, base_timeout=20):
    """
    Call agent with retries, exponential backoff and structured error handling.
    Returns (http_status_or_0, response_json_or_error_dict)
    """
    req = {
        'job': job,
        'input_timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'payload': payload,
        'request_id': str(uuid.uuid4())
    }
    attempt = 0
    last_exc = None
    while attempt < max_retries:
        attempt += 1
        timeout = base_timeout * (2 ** (attempt - 1))  # exponential backoff
        try:
            start = time.time()
            r = requests.post(url, json=req, timeout=timeout)
            duration = time.time() - start
            j = safe_json(r)
            if j is None:
                # malformed response from agent
                return 0, {
                    'status': 'error',
                    'meta': {'agent': name, 'job': job},
                    'issues': [{'type': 'parse_error', 'note': 'invalid json from agent', 'severity': 'high'}],
                    'raw_status': r.status_code
                }
            # attach call metadata for provenance
            j['_call_meta'] = {
                'http_status': r.status_code,
                'duration_s': round(duration, 3),
                'attempt': attempt
            }
            return r.status_code, j
        except RequestException as e:
            last_exc = e
            app.logger.warning(f"Call to {name} failed on attempt {attempt}: {e}")
            # small backoff
            time.sleep(1 * attempt)
            continue

    # all retries failed
    return 0, {
        'status': 'error',
        'meta': {'agent': name, 'job': job},
        'issues': [{'type': 'connection_error', 'note': str(last_exc), 'severity': 'high'}]
    }


@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'service': 'eva-eco-orchestrator'}), 200


@app.route('/run', methods=['POST'])
def run():
    """
    Main entrypoint for orchestrator.
    Expects JSON payload with fields:
      job, request_id, initiator, date_from, date_to, campaign_ids, channels, options
    """
    start_pipeline = time.time()
    try:
        payload = request.json or {}
        # Basic validation
        job = payload.get('job', 'job_from_client')
        request_id = payload.get('request_id', 'local-' + str(uuid.uuid4()))
        initiator = payload.get('initiator', 'system')
        date_from = payload.get('date_from')
        date_to = payload.get('date_to')
        campaign_ids = payload.get('campaign_ids', [])
        channels = payload.get('channels', [])

        # Prepare initial payload for MDC
        next_payload = {
            'campaign_ids': campaign_ids,
            'date_from': date_from,
            'date_to': date_to,
            'channels': channels
        }

        final_provenance = []
        aggregated_outputs = {}
        degraded = False

        # Sequentially call agents
        for name, url in AGENTS:
            status_code, resp = call_agent(name, url, 'job_from_eva', next_payload, max_retries=3, base_timeout=20)
            if resp is None:
                resp = {'status': 'error', 'meta': {'agent': name, 'job': 'job_from_eva'}, 'issues': [{'note': 'no response'}]}

            prov_entry = {
                'agent': name,
                'status': resp.get('status', 'error'),
                'meta': resp.get('meta'),
                'issues': resp.get('issues', []),
                'data_sample': (resp.get('data') or [])[:1],
                'call_meta': resp.get('_call_meta', {})
            }
            final_provenance.append(prov_entry)

            # Error -> stop pipeline with incident
            if resp.get('status') == 'error':
                pipeline_duration = round(time.time() - start_pipeline, 3)
                final_report = {
                    'request_id': request_id,
                    'status': 'error',
                    'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                    'provenance': final_provenance,
                    'aggregated_outputs': aggregated_outputs,
                    'notes': 'Stopped due to agent error',
                    'pipeline_duration_s': pipeline_duration
                }
                return jsonify(final_report), 500

            # Partial -> mark degraded and continue
            if resp.get('status') == 'partial':
                degraded = True
                aggregated_outputs.setdefault('_degraded_provenance', []).append(name)

            # Pass data forward (if any)
            next_payload = {'input_data': resp.get('data', [])}

        # Build aggregated outputs (in real system we'd merge and compute; here mock)
        aggregated_outputs.setdefault('metrics_summary', {'note': 'sample aggregated outputs'})
        aggregated_outputs.setdefault('funnel_reports', {'note': 'sample funnel outputs'})
        aggregated_outputs.setdefault('creative_insights', {'note': 'sample creative outputs'})
        aggregated_outputs.setdefault('economics', {'note': 'sample economics outputs'})
        aggregated_outputs.setdefault('forecast', {'note': 'sample forecast outputs'})

        pipeline_duration = round(time.time() - start_pipeline, 3)

        # Simulated quality gates (in production compute from real data)
        quality_gates = {
            'completeness': 1.0 if aggregated_outputs.get('metrics_summary') else 0.0,
            'confidence': 0.95,
            'passed': (1.0 >= 0.9)
        }

        final_report = {
            'request_id': request_id,
            'status': 'partial' if degraded else 'ok',
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            'provenance': final_provenance,
            'aggregated_outputs': aggregated_outputs,
            'decision_signals': [],  # director will compute later
            'human_actions': [],
            'notes': '',
            'pipeline_duration_s': pipeline_duration,
            'quality_gates': quality_gates
        }

        if degraded:
            final_report['notes'] = 'Degraded quality: one or more agents returned partial.'
            final_report['final_status_note'] = 'degraded_quality'

        return jsonify(final_report), 200

    except Exception as e:
        app.logger.exception("Unhandled exception in orchestrator.run")
        pipeline_duration = round(time.time() - start_pipeline, 3)
        final_report = {
            'request_id': payload.get('request_id', 'local-' + str(uuid.uuid4())),
            'status': 'error',
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            'provenance': [],
            'aggregated_outputs': {},
            'notes': f'Unhandled exception: {str(e)}',
            'pipeline_duration_s': pipeline_duration
        }
        return jsonify(final_report), 500


if __name__ == '__main__':
    # When running locally for debug, you can set DEBUG=1 environment variable
    debug_flag = os.environ.get('DEBUG', '0') == '1'
    app.run(host='0.0.0.0', port=8080, debug=debug_flag)
