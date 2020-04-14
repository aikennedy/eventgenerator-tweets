var http = require('http');
var https = require('https');
var fs = require('fs');
var ejs = require('ejs');
var { parse } = require('querystring');
var cron = require('node-cron');
var utf8 = require('utf8');
var crypto = require('crypto');
var syncrequest = require('sync-request');
var asyncrequest = require('then-request');

// Load tweets, etc. from JSON file
function loadFromFile(loadstring) {
	var response = syncrequest('GET', 'https://tgazureworkshopblob.blob.core.windows.net/tweets/' + loadstring + '.json');
	return JSON.parse(response.getBody('utf8'));
}

var tweets = loadFromFile('tweets');
var users = loadFromFile('users');
var brands = loadFromFile('brands');
var settings = loadFromFile('settings');
var instances = [];

var toReplace = settings.replaceString;
var regexExp = new RegExp(toReplace, "gi")

// Set to reload tweets every minute
cron.schedule('* * * * *', () => {
	asyncrequest('GET', 'https://tgazureworkshopblob.blob.core.windows.net/tweets/tweets.json').done(function (tweetsResponse) {
		if (tweetsResponse.statusCode < 300) {
			tweets = JSON.parse(tweetsResponse.getBody('utf8'));
		}
	});
	asyncrequest('GET', 'https://tgazureworkshopblob.blob.core.windows.net/tweets/users.json').done(function (usersResponse) {
		if (usersResponse.statusCode < 300) {
			users = JSON.parse(usersResponse.getBody('utf8'));
		}
	});
	asyncrequest('GET', 'https://tgazureworkshopblob.blob.core.windows.net/tweets/brands.json').done(function (brandsResponse) {
		if (brandsResponse.statusCode < 300) {
			brands = JSON.parse(brandsResponse.getBody('utf8'));
		}
	});
	asyncrequest('GET', 'https://tgazureworkshopblob.blob.core.windows.net/tweets/settings.json').done(function (settingsResponse) {
		if (settingsResponse.statusCode < 300) {
			settings = JSON.parse(settingsResponse.getBody('utf8'));
		}
	});
	toReplace = settings.replaceString;
});

// Function to create autorization header for Event Hub
function createSharedAccessToken(uri, saName, saKey) { 
    if (!uri || !saName || !saKey) { 
            throw "Missing required parameter"; 
        } 
    var encoded = encodeURIComponent(uri); 
    var now = new Date(); 
    var week = 60*60*24*7;
    var ttl = Math.round(now.getTime() / 1000) + week;
    var signature = encoded + '\n' + ttl; 
    var signatureUTF8 = utf8.encode(signature); 
    var hash = crypto.createHmac('sha256', saKey).update(signatureUTF8).digest('base64'); 
    return 'SharedAccessSignature sr=' + encoded + '&sig=' +  
        encodeURIComponent(hash) + '&se=' + ttl + '&skn=' + saName; 
}

// Sample configuration information
/*
var namespace1 = "tg-ak-testeventhub";
var eventHubName1 = "tgaktesteventhub01";
var accessPolicyName1 = "iotdevice01";
var accessPolicyKey1 = "/oZrtZuShHtzybl/IXYCyy0qCvePH38RL36rsoKa1fA=";
*/


http.createServer(function (request, response) {
	
	// If request is POST and submitted by HTML form, then try to add the event hub instance
	const FORM_URLENCODED = 'application/x-www-form-urlencoded';
	if (request.method === 'POST' && request.headers['content-type'] === FORM_URLENCODED) {
		let body = '';
		request.on('data', chunk => {
			body += chunk.toString();
		});
		request.on('end', () => {
			var postvars = parse(body);
			if (postvars.startStop == "start") {
				instances.push({
					namespace: postvars.eventHubNamespace,
					eventHubName: postvars.eventHubName,
					accessPolicyName: postvars.accessPolicyName,
					accessPolicyKey: postvars.accessPolicyKey,
					failures: 0
				});
			} else if (postvars.startStop == "stop") {
				for (var i = instances.length - 1; i >= 0; --i) {
					if (instances[i].namespace == postvars.eventHubNamespace && instances[i].eventHubName == postvars.eventHubName) {
						instances.splice(i,1);
					}
				}
			}
		});
	};
	
	// Render HTML page with form to add a new instance
	fs.readFile('index.html', 'utf-8', function(err, data) {
	
		var feedback = '';
		for (var i = 0, len = instances.length; i < len; i++) {
			feedback += `<li>${instances[i].namespace}.servicebus.windows.net/${instances[i].eventHubName}`;
			if (instances[i].failures >= 3) {
				feedback += ' <b>Failed 3 times. Abandoned.</b>';
			}
			feedback += '</li>';
		};
		
		response.writeHead(200, {'Content-Type': 'text/html'});
		response.write(ejs.render(data, {feedback: feedback}));
		
		response.end();
	});
	
}).listen(process.env.PORT || 8081);

// Console will print a message to confirm the app is running
console.log('Server running');

var t = 0;
cron.schedule('* * * * * *', () => {
	// Tweet roughly every 2 seconds
	if (Math.random() > settings.probabilityPerSecond) { 
	  // skip these iterations
	} else {
		
		// Prepare the random tweet properties
		var timestamp_date = new Date();
		var timestamp_str = timestamp_date.toString();
		var timestamp = timestamp_str.substr(0, 10) + timestamp_str.substr(15, 18) + timestamp_str.substr(10, 5);
		var tweetRand = Math.floor(Math.random()*tweets.length);
		var brandRand = Math.floor(Math.random()*brands.length);
		var userRand = Math.floor(Math.random()*users.length);
		var user = users[userRand];
		var brand = brands[brandRand];
		var tweetText = tweets[tweetRand].text.replace(regexExp, brand.tag);
		var retweeted = Math.random() < settings.probabilityIsRetweet;
		if (retweeted) {
			tweetText = "RT " + tweetText;
		};
		
		// Build the tweet object
		if (settings.flattenOutputJson) {
			var tweet = tweets[tweetRand];
			tweet.text = tweetText;
			tweet.user = user.name;
			tweet.brand = brand.tag;
			tweet.created_at = timestamp;
			tweet.retweeted = retweeted;			
		} else {
			var tweet = tweets[tweetRand];
			tweet.text = tweetText;
			tweet.user = user;
			tweet.matching_rules = brand;
			tweet.created_at = timestamp;
			tweet.retweeted = retweeted;
		};
		
		console.log(JSON.stringify(tweet));
		
		// Send the tweet to each registered instance
		for (var i = 0, len = instances.length; i < len; i++) {
			//skip any instances that have failed three times
			if (instances[i].failures >= 3) {
				continue;
			}
			(function(i){ 
				//create new variable with loop counter, so it can be referenced in internal functions
				var ival = i;  
			
				var namespace = instances[ival].namespace;
				var eventHubName = instances[ival].eventHubName;
				var accessPolicyName = instances[ival].accessPolicyName;
				var accessPolicyKey = instances[ival].accessPolicyKey;
		
				var sharedAccessToken = createSharedAccessToken(namespace + '.servicebus.windows.net/' + eventHubName, accessPolicyName, accessPolicyKey);
		
				var options = {
					hostname: namespace + '.servicebus.windows.net',
					port: 443,
					path: '/' + eventHubName + '/messages?timeout=60&api-version=2014-01',
					method: 'POST',
					headers: {
						'Authorization': sharedAccessToken,
						'Content-Type': 'application/atom+xml;type=entry;charset=utf-8',
						'Host': namespace + '.servicebus.windows.net'
					}
				};
				
				var req = https.request(options, (res) => {
					console.log(`statusCode: ${res.statusCode}`);
					if (res.statusCode == 201) {
						instances[ival].failures = 0;
					} else {
						instances[ival].failures++;
						console.log(instances[ival].failures + ' failures');
					};
				
					res.on('data', (d) => {
						process.stdout.write(d)
					});
				});
		
				req.on('error', (error) => {
					console.error(error);
				});
				req.write(JSON.stringify(tweet));
				req.end();
			})(i);
		}
		t++;
		
	}
});