from flask import Flask, request, jsonify
import os, time

app = Flask(__name__)
AGENT = os.environ.get('AGENT_NAME','MOCK')

@app.route('/run', methods=['POST'])
def run():
    req = request.json or {}
    job = req.get('job')
    # simple canned responses depending on agent
    base = {
        'status':'ok',
        'meta':{'agent':AGENT,'job':job,'timestamp':time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),'version':'1.0.0'},
        'data': [],
        'issues': [],
        'artifact': None,
        'text_report': ''
    }
    if AGENT == 'MDC':
        base['data'] = [
            {'campaign_id':101,'date':req.get('payload',{}).get('date_from'),'channel':'email','metrics':{'impressions':1000,'clicks':50,'conversions':5},'confidence':0.95}
        ]
    elif AGENT == 'MAR':
        base['data'] = [
            {'campaign_id':101,'metrics_summary':{'impressions':1000,'clicks':50,'ctr':0.05}}
        ]
    elif AGENT == 'CFA':
        base['data'] = [{'funnel_summary':[{'step':'page_view','users':1000},{'step':'purchase','users':50}]}]
    elif AGENT == 'CPS':
        base['data'] = [{'creative_scores':[{'creative_id':'email_001','score':0.8}]}]
    elif AGENT == 'MBO':
        base['data'] = [{'economics_summary':[{'campaign_id':101,'spend':120,'revenue':360,'roas':3.0}]}]
    elif AGENT == 'FTM':
        base['data'] = [{'forecast':[{'date':'2025-12-01','impressions':1100}]}]
    else:
        base['data'] = [{'note':'generic mock response'}]
    return jsonify(base)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
