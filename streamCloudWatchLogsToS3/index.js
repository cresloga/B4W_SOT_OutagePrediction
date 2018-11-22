var AWS = require('aws-sdk');
var zlib = require('zlib');

exports.handler = (event, context, callback) => {
    var bucketName = process.env.BUCKET_NAME;
    var buf = new Buffer(event.awslogs.data, 'base64');
    zlib.unzip(buf, function (err, buffer) {          // decompress the content
        if (err) throw err;                             // handle decompression error
        var content = buffer.toString('utf8');          // encode in utf-8
        var timestamp, result = -1;
        var final_data = {};
        content = JSON.parse(content);
        if (content && content.logEvents) {
            for (var i = 0; i < content.logEvents.length; i++) {
        	    if (content.logEvents[i].message.indexOf("Rest Api Response") != -1) {
        		    timestamp = content.logEvents[i].message.substring(0,19);
            		if (content.logEvents[i].message.indexOf("statusCode") != -1) {
            			result = 1;
            		} else if (content.logEvents[i].message.indexOf("errorMessage") != -1) {
            			result = 0;
            		} else {
            			result = -1;
            		}
            		var now = new Date();
                    var fileName = "LogTask_"+now.getTime().toString();
    	            final_data.timestamp = timestamp;
                    final_data.result = result;
                    putObjectToS3(bucketName,fileName, JSON.stringify(final_data));
    	        }
            }
        }
    });
};

function putObjectToS3(bucket, key, data){
    var s3 = new AWS.S3();
    var params = {
        Bucket : bucket,
        Key : key,
        Body : data
    }
    s3.putObject(params, function(err, data) {
      if (err) console.log(err, err.stack); // an error occurred
      else     console.log(data);           // successful response
    });
}