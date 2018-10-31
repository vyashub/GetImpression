import json
import os
import pyodbc

from flask import Flask
from flask import request
from flask import make_response

# Flask app should start in global layout
app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    req = request.get_json(silent=True, force=True)
    print(json.dumps(req, indent=4))
    
    res = makeResponse(req)
    
    res = json.dumps(res, indent=4)
    # print(res)
    r = make_response(res)
    r.headers['Content-Type'] = 'application/json'
    return r

def makeResponse(req):
    result = req.get("result")
    parameters = result.get("parameters")
    cmp_id = parameters.get("campaignid")
	
    # establish a connection using the DSN you created earlier
    conn = pyodbc.connect("DSN=Databricks_DSN", autocommit = True)
    
    # run a SQL query using the connection you created
    cursor = conn.cursor()
    print(cursor)
    print('start')
    cmp_id = 'nlsn1234'
    query = '''SELECT nielsen_campaign_id as Campaign_id,nielsen_campaign_name as Campaign_name,
    encrypted_nielsen_campaign_id as encrypted_campaign_id,
    collection_start_date, collection_end_date,
    campaign_start_date,campaign_end_date,
    targeted_gender_type_code,targeted_start_age,
    targeted_end_age, load_id 
    FROM digital_dar_basic_prod.CAMPAIGN
    WHERE NIELSEN_CAMPAIGN_ID = "{cmp_id}"
    AND RELEASED_FOR_PROCESSING_FLAG="Y"
    '''.format(cmp_id=cmp_id)
    
    cursor.execute(query)
    
    print('Done')
    
    rows = cursor.fetchall()
    # print the rows retrieved by the query.
    speech = 'The number of impressions for id 11111 is2222222'
    for row in rows:
        print(row)
        speech = "The number of impression for "+cmp_id+" is "+ row[-1]
        
    return {
    "speech": speech,
    "displayText": speech,
    "source": "apiai-weather-webhook"
    }

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    print("Starting app on port %d" % port)
    app.run(debug=False, port=port, host='0.0.0.0')
