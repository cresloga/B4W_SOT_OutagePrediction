const aws = require('aws-sdk');
const https = require('https');

exports.handler = (event, context, callback) => {
    
    let eventTime = event.Records[0].eventTime;
    let fileName = event.Records[0].s3.object.key;
    
    const S3_BUCKET = process.env.S3_BUCKET;

    const awsS3Config = 
    {
      region : process.env.S3_REGION
    };
    
     getS3ObjectData(fileName, S3_BUCKET, awsS3Config).then(function(data){
         console.log("S3 Object Data : "+JSON.stringify(data));
         if(data.result == 0){
             getListOfObjectsInLastOneMinute(eventTime, S3_BUCKET, awsS3Config).then(function(result){
              var denominator = result.length + 1;
              console.log("Denominator : "+denominator);
              getFaiureObjects(result, S3_BUCKET, awsS3Config).then(function(failedObjects){
                  var numerator = failedObjects.length + 1;
                  console.log("Numerator : "+numerator);
                  let percentageError = (numerator/denominator)*100;
                  console.log("Percentage Error : "+percentageError);
                  isItOutage(percentageError.toString(), process.env.MODEL_ID, process.env.PREDICT_END_POINT_URL).then(function(predictionResult){
                      console.log("Prediction Result : "+JSON.stringify(predictionResult));
                      let predictionScore = predictionResult.Prediction.predictedLabel;
                      if(predictionScore == 1){
                          postToSlack(percentageError, callback);
                      }
                      else{
                          callback(null,"No Outage Predicted. Do nothing");
                      }
                  },
                  function(errorInPrediction){
                      console.log("Error in Prediction : "+errorInPrediction);
                      callback(null, JSON.stringify(errorInPrediction));
                  });
                  
              }, function(errorInGettingFailedObjects){
                  console.log(errorInGettingFailedObjects);
                  callback(null, JSON.stringify(errorInGettingFailedObjects));
              });
            }, function(error){
                console.log(error);
            });
         }
     }, function(err){
         console.log(err);
     });
};

function postToSlack(percentageError, callback){
    const payload = JSON.stringify({
        text: 'Outage in Troubleshoot service predicted. '+percentageError+"% failure in service call to Troubleshoot service in last 1 minute"
      });
  
    const options = {
        hostname: process.env.SLACK_HOST_NAME,
        method: "POST",
        path: process.env.SLACK_WEBHOOK_URI
      };
  
    const req = https.request(options,
          (res) => res.on("data", () => callback(null, "Outage Prediction posted to Slack Channel #outage")))
      req.on("error", (error) => callback(JSON.stringify(error)));
      req.write(payload);
      req.end();
}

function isItOutage(dataKey, modelId, endpointUrl){
    
    return new Promise((resolve, reject) => {
        var params = {
          MLModelId: modelId, /* required */
          PredictEndpoint: endpointUrl, /* required */
          Record: { /* required */
            'Percentage Errors': dataKey
            /* '<VariableName>': ... */
          }
        };
        var machinelearning = new aws.MachineLearning();
        machinelearning.predict(params, function(errorInPrediction, predictionResult) {
          if (errorInPrediction) {
              console.log(errorInPrediction, errorInPrediction.stack); // an error occurred
              reject(errorInPrediction);
          }
          else{
              resolve(predictionResult);
          }
        });
    });
}

function getFaiureObjects(listOfObjects, s3Bucket, awsS3Config){
    console.log("Entering getFaiureObjects");
    var listOfFailureObjects=[];
    let numOfFailureObjects = 0;
    return new Promise((resolve, reject) => {
        for(let i=0; i<listOfObjects.length; i++){
             getS3ObjectData(listOfObjects[i].key, s3Bucket, awsS3Config).then(function(data){
                 console.log("S3 Object Failure Data : "+JSON.stringify(data));
                 if(data.result == 0){
                     listOfFailureObjects[numOfFailureObjects] = listOfObjects[i];
                     numOfFailureObjects++;
                 }
             }, function(error){
                 console.log("Error Fetching Failure Object Data : "+error);
                 reject(error);
             });
        }
        resolve(listOfFailureObjects);
    });
}

function getS3ObjectData(fileName, s3Bucket, awsS3Config){
    console.log("Entering getS3ObjectData");
    const params = {
        Bucket: s3Bucket,
        Key: fileName,
    };
    const s3 = new aws.S3(awsS3Config);
    console.log("Exiting getS3ObjectData");
    return new Promise((resolve, reject) => {
        s3.getObject(params).promise().then(function(result){
            var content = result.Body.toString();
            content = JSON.parse(content);
            resolve(content);
        },
        function (error){
            reject(error);
        });
    });
}

function getListOfObjectsInLastOneMinute(eventTime, s3Bucket, awsS3Config){
    console.log("Entering getListOfObjectsInLastOneMinute");
    const s3 = new aws.S3(awsS3Config);
    const params = {Bucket: s3Bucket};
    return new Promise((resolve, reject) => {
         s3.listObjects(params, function(err, data){
            if(err){
                console.log("error : "+err);
                console.log("Exiting getListOfObjectsInLastOneMinute");
                reject(err);
            }
            else{
                var listOfObjects=[];
                var bucketContents = data.Contents;
                console.log("Bucket Size :"+bucketContents.length);
                var numOfEligibleObjects = 0;
                for(let i=0; i<bucketContents.length; i++){
                  let key = {'key': bucketContents[i].Key};
                  let objectCreationTime = bucketContents[i].LastModified;
                  let timeDifference = (new Date(eventTime) - new Date(objectCreationTime))/(60*1000);
                  if(timeDifference>0 && timeDifference<1){
                    listOfObjects[numOfEligibleObjects]=key;  
                    numOfEligibleObjects++;
                  }
                }  
                console.log(listOfObjects);
                console.log("Exiting getListOfObjectsInLastOneMinute");
                resolve(listOfObjects);
            }
        });
    });
    
}