from flask import Flask, request, jsonify
import requests, os, time, uuid

app = Flask(__name__)
AGENTS = [
    ('mdc', 'http://mdc:80/run'),
    ('mar', 'http://mar:80/run'),
    ('cfa', 'http://cfa:80/run'),
    ('cps', 'http://cps:80/run'),
    ('mbo', 'http://mbo:80/run'),
    ('ftm', 'http://ftm:80/run'),
]

def call_agent(name, url, job, payload):
    req = {
        'job': job,
        'input_timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'payload': payload,
        'request_id': str(uuid.uuid4())
    }
    try:
        r = requests.post(url, json=req, timeout=20)
        return r.status_code, r.json()
    except Exception as e:
        return 0, {'status':'error','meta':{'agent':name,'job':job},'issues':[{'note':str(e)}]}

@app.route('/run', methods=['POST'])
def run():
    payload = request.json or {}
    request_id = payload.get('request_id','local-'+str(uuid.uuid4()))
    date_from = payload.get('date_from')
    date_to = payload.get('date_to')
    campaign_ids = payload.get('campaign_ids',[])
    final_provenance = []
    aggregated_outputs = {}
    # 1) Call MDC
    # We'll pass a simple payload to each agent: input_data / partial results
    next_payload = {'campaign_ids':campaign_ids, 'date_from':date_from, 'date_to':date_to}
    for name, url in AGENTS:
        status_code, resp = call_agent(name, url, 'job_from_eva', next_payload)
        if resp is None:
            resp = {'status':'error','meta':{'agent':name},'issues':[{'note':'no response'}]}
        final_provenance.append({
            'agent': name,
            'status': resp.get('status','error'),
            'meta': resp.get('meta'),
            'issues': resp.get('issues',[]),
            'data_sample': (resp.get('data') or [])[:1]
        })
        # For demo, feed the same resp.data to next agent as payload.input_data
        next_payload = {'input_data': resp.get('data',[])}
        # If agent error -> stop and return incident
        if resp.get('status') == 'error':
            final_report = {
                'request_id': request_id,
                'status': 'error',
                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                'provenance': final_provenance,
                'aggregated_outputs': aggregated_outputs,
                'notes': 'Stopped due to agent error'
            }
            return jsonify(final_report), 500
    # build dummy aggregated outputs
    aggregated_outputs['metrics_summary'] = {'note':'sample aggregated outputs'}
    final_report = {
        'request_id': request_id,
        'status': 'ok',
        'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'provenance': final_provenance,
        'aggregated_outputs': aggregated_outputs
    }
    return jsonify(final_report), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
