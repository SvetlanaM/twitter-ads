import pprint, json, os, datetime, urllib, pip, gzip, csv
from keboola import docker
from time import sleep
import oauth2, requests, parsedatetime

pip.main(['install', 'oauth2'])
pip.main(['install', 'requests'])
pip.main(['install', 'parsedatetime'])

cal = parsedatetime.Calendar()

cfg = docker.Config('/data/')
parameters = cfg.get_parameters()
config = {}
configFields = ['bucket', 'consumer_key', '#consumer_secret', 'api_key', '#api_secret', 'since', 'until', 'midnight_in_utc', 'account_ids']
dateCounter = 0
dates = {}
now = since
now += datetime.timedelta(days=1)

for field in configFields:
	config[field] = parameters.get(field)

	if not config[field]:
		raise Exception('Missing mandatory configuration field: '+field)

since, _ = cal.parseDT(datetimeString=str(config['since']))
until, _ = cal.parseDT(datetimeString=str(config['until']))
sinceFormatted = since.strftime("%Y-%m-%d")+"T"+config['midnight_in_utc']+":00:00Z"
untilFormatted = until.strftime("%Y-%m-%d")+"T"+config['midnight_in_utc']+":00:00Z"

while now <= until:
	dates[dateCounter] = now.strftime("%Y-%m-%d")
	now += datetime.timedelta(days=1)
	dateCounter += 1

def oauth_req(url, http_method="GET", post_body="", http_headers=None):
	encoding = 'utf-8'
	url = url.encode(encoding)
	consumer = oauth2.Consumer(key=config['consumer_key'].encode(encoding), secret=config['#consumer_secret'].encode(encoding))
	token = oauth2.Token(key=config['api_key'].encode(encoding), secret=config['#api_secret'].encode(encoding))
	client = oauth2.Client(consumer, token)
	content = client.request( url, method=http_method.encode(encoding), body=post_body.encode(encoding), headers=http_headers)
	return content.decode('utf-8')

def downloadFile(url):
	with open('response.gz', 'wb') as handle:
		r = requests.get(url)

		for block in r.iter_content(1024):
			handle.write(block)

def processJson(account, urlToDownload):
	downloadFile(urlToDownload)
	metrics = ['app_clicks','billed_charge_local_micro','billed_engagements','card_engagements','carousel_swipes','clicks','engagements','follows','impressions','likes','qualified_impressions','replies','retweets','tweets_send','url_clicks']
	header = ['account_name','account_id', 'campaign_id','date']
	header += metrics

	writeHeader = True
	if os.path.isfile("/data/out/tables/campaigns.csv"):
		writeHeader = False

	writer = csv.writer(open("/data/out/tables/campaigns.csv", 'a'))

	if writeHeader == True:
		writer.writerow(header)

	with gzip.open('response.gz', 'r') as textFile:
		data = textFile.read().decode("utf-8").replace('\n', '')
	response = json.loads(data)

	for c in response['data']:
		root = c['id_data'][0]['metrics']

		for index, date in dates.iteritems():
			row = [account['name'], account['id'], c['id'], date]
			for metric in metrics:
				if root[metric]:
					row.append(root[metric][index])
				else:
					row.append(0)

			writer.writerow(row)

# Get data for 20 campaigns
def processCampaigns(account, campaigns, since, until):
	print("Downloading info for 20 campaigns of account: "+account['name']+" - "+account['id'])

	campaignIds = []
	for c in campaigns:
		campaignIds.append(c['id'])

	params = {
		"entity": "CAMPAIGN",
		"entity_ids": ",".join(campaignIds),
		"granularity": "DAY",
		"metric_groups": "ENGAGEMENT,BILLING",
		"placement": "ALL_ON_TWITTER",
		"start_time": since,
		"end_time": until,
	}

	# Create Job
	url = 'https://ads-api.twitter.com/1/stats/jobs/accounts/'+account['id']+'?'+urllib.urlencode(params)
	response = json.loads(oauth_req(url, 'POST').decode('utf-8'))
	jobId = response['data']['id']

	sleep(10)

	# Check job status
	url = 'https://ads-api.twitter.com/1/stats/jobs/accounts/'+account['id']+'?job_ids='+str(jobId)
	response = json.loads(oauth_req(url).decode('utf-8'))

	urlToDownload = ''

	if 'url' in response['data'][0]:
		urlToDownload = response['data'][0]['url']

	jobCounter = 0

	# Check job status if it is not done yet
	while response['data'][0]['status'] != 'SUCCESS' and jobCounter <= 200:
		url = 'https://ads-api.twitter.com/1/stats/jobs/accounts/'+account['id']+'?job_ids='+str(jobId)
		response = json.loads(oauth_req(url).decode('utf-8'))

		if 'url' in response['data'][0]:
			urlToDownload = response['data'][0]['url']

		sleep(20)
		jobCounter += 1

	if not urlToDownload:
		pprint.pprint(response)
		raise Exception("Job did not completed successfully or there is nothing to get.")

	processJson(account, campaigns, urlToDownload)

# Get campaigns of account and process them by step 20
def getData(since, until):
	url ='https://ads-api.twitter.com/1/accounts/12167915/campaigns?sort_by=end_time-desc&count=100'
	response = json.loads(oauth_req(url))

	campaigns = []

	for c in response['data']:
		campaigns.append(c)

		if len(campaigns) == 20:
			processCampaigns(account, campaigns, since, until)
			campaigns = []

	if len(campaigns) > 0:
		processCampaigns(account, campaigns, since, until)

# Main Flow
for account in config['account_ids']:
	getData(sinceFormatted, untilFormatted)
