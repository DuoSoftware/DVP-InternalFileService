/**
 * Created by pawan on 3/26/2015.
 */

var redis=require('redis');
//var messageFormatter = require('./DVP-Common/CommonMessageGenerator/ClientMessageJsonFormatter.js');


var config = require('config');
var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;



var port = config.Redis.port || 3000;
var ip = config.Redis.ip;
var password = config.Redis.password;


var client = redis.createClient(port,ip);
client.auth(password, function (error) {
    console.log("Redis Auth Error " + error);
});

client.on("error", function (err) {
    console.log("Error " + err);


});




function RedisPublish(SID,AID,reqId,callback)
{

if(client.connected)
{
    logger.info('[DVP-FIleService.UploadFile.RedisPublisher] - [%s] - [REDIS] - Redis client is Online',reqId);


            try{
                client.publish("CSCOMMAND:"+SID+":downloadfile",AID,function(err,reply)
                {
                    if(err)
                    {
                        logger.error('[DVP-FIleService.UploadFile.RedisPublisher] - [%s] - [REDIS] - Error occurred while publishing to redis - CSCOMMAND:%s:downloadfile  - > %s',reqId,SID,AID,err);
                        callback(err,undefined);
                    }
                    else
                    {
                        logger.info('[DVP-FIleService.UploadFile.RedisPublisher] - [%s] - [REDIS] - Redis publishing to Succeeded - CSCOMMAND:%s:downloadfile  - > %s',reqId,SID,AID);
                        callback(undefined,reply);
                    }
                });
            }
            catch(ex)
            {
                logger.error('[DVP-FIleService.UploadFile.RedisPublisher] - [%s] - [REDIS] - Exception occurred while publishing to redis ',reqId,ex);
                callback(ex,undefined);
            }


}
    else
{
    logger.error('[DVP-FIleService.UploadFile.RedisPublisher] - [%s] - [REDIS] - Redis client is not available ',reqId);
    callback(new Error('Redis Client is not available'),undefined);
}


}


module.exports.RedisPublish = RedisPublish;

