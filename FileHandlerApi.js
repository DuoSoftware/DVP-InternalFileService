
var path = require('path');
var uuid = require('node-uuid');
var DbConn = require('dvp-dbmodels');
var config = require('config');
var streamifier = require('streamifier');
var fs=require('fs');


//Sprint 5

var couchbase = require('couchbase');
var Cbucket=config.Couch.bucket;
var CHip=config.Couch.ip;

var cluster = new couchbase.Cluster("couchbase://"+CHip);
var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;


var mongodb = require('mongodb');
var Server = require('mongodb').Server,
    Code = require('mongodb').Code;



console.log('\n.............................................File handler Starts....................................................\n');


//log done...............................................................................................................

function DownloadFileByID(res,UUID,display,option,Company,Tenant,reqId,callback)
{
    if(UUID)
    {
        try {

            logger.debug('[DVP-FIleService.DownloadFile] - [%s] - Searching for Uploaded file %s',reqId,UUID);
            DbConn.FileUpload.find({where: [{UniqueId: UUID},{CompanyId:Company},{TenantId:Tenant}]}).then(function (resUpFile) {

                if (resUpFile) {


                    if(option=="MONGO")
                    {

                        logger.debug('[DVP-FIleService.DownloadFile] - [%s] - [MONGO] - Downloading from Mongo',reqId,JSON.stringify(resUpFile));

                        var extArr=resUpFile.FileStructure.split('/');
                        var extension=extArr[1];

                        var uri = 'mongodb://'+config.Mongo.user+':'+config.Mongo.password+'@'+config.Mongo.ip+'/'+config.Mongo.dbname;

                        mongodb.MongoClient.connect(uri, function(error, db)
                        {
                            console.log(uri);
                            console.log("Error1 "+error);
                            if(error)
                            {
                                res.status(400);
                                res.end();
                            }
                            else
                            {
                                var bucket = new mongodb.GridFSBucket(db, {
                                    chunkSizeBytes: 1024
                                });


                                bucket.openDownloadStreamByName(UUID).
                                    pipe(res).
                                    on('error', function(error) {
                                        console.log('Error !'+error);
                                        res.status(400);
                                        res.end();

                                    }).
                                    on('finish', function() {
                                        console.log('done!');
                                        res.status(200);
                                        res.end();

                                    });
                            }



                        });



                    }
                    else if(option=="COUCH")
                    {
                        logger.debug('[DVP-FIleService.DownloadFile] - [%s] - [MONGO] - Downloading from Couch',reqId,JSON.stringify(resUpFile));

                        var bucket = cluster.openBucket(Cbucket);

                        bucket.get(UUID, function(err, result) {
                            if (err)
                            {
                                console.log(err);

                                callback(err,undefined);
                                res.status(400);
                                res.end();
                            }else
                            {
                                console.log(resUpFile.FileStructure);
                                res.setHeader('Content-Type', resUpFile.FileStructure);

                                var s = streamifier.createReadStream(result.value);

                                s.pipe(res);


                                s.on('end', function (result) {

                                    logger.debug('[DVP-FIleService.DownloadFile] - [%s] - [FILEDOWNLOAD] - Streaming succeeded',reqId);
                                    SaveDownloadDetails(resUpFile,reqId,function(errSv,resSv)
                                    {
                                        if(errSv)
                                        {
                                            callback(errSv,undefined);
                                        }
                                        else
                                        {
                                            callback(undefined,resSv);
                                        }
                                    });


                                    console.log("ENDED");
                                    res.status(200);
                                    res.end();
                                });
                                s.on('error', function (err) {

                                    logger.error('[DVP-FIleService.DownloadFile] - [%s] - [FILEDOWNLOAD] - Error in streaming',reqId,err);
                                    console.log("ERROR");
                                    res.status(400);
                                    res.end();
                                });

                            }


                        });
                    }
                    else
                    {
                        logger.debug('[DVP-FIleService.DownloadFile] - [%s] - [PGSQL] - Record found for File upload %s',reqId,JSON.stringify(resUpFile));
                        try {
                            res.setHeader('Content-Type', resUpFile.FileStructure);
                            var SourcePath = (resUpFile.URL.toString()).replace('\',' / '');
                            logger.debug('[DVP-FIleService.DownloadFile] - [%s]  - [FILEDOWNLOAD] - SourcePath of file %s',reqId,SourcePath);

                            logger.debug('[DVP-FIleService.DownloadFile] - [%s]  - [FILEDOWNLOAD] - ReadStream is starting',reqId);
                            var source = fs.createReadStream(SourcePath);

                            source.pipe(res);
                            source.on('end', function (result) {
                                logger.debug('[DVP-FIleService.DownloadFile] - [%s] - [FILEDOWNLOAD] - Piping succeeded',reqId);
                                res.status(200);
                                res.end();
                            });
                            source.on('error', function (err) {
                                logger.error('[DVP-FIleService.DownloadFile] - [%s] - [FILEDOWNLOAD] - Error in Piping',reqId,err);
                                res.status(400);
                                res.end();
                            });
                        }
                        catch(ex)
                        {
                            logger.error('[DVP-FIleService.DownloadFile] - [%s] - [FILEDOWNLOAD] - Exception occurred when download section starts',reqId,ex);

                            callback(ex, undefined);
                            res.status(400);
                            res.end();
                        }
                    }

                }

                else {
                    logger.error('[DVP-FIleService.DownloadFile] - [%s] - [PGSQL] - No record found for  Uploaded file  %s',reqId,UUID);
                    callback(new Error('No record for id : ' + UUID), undefined);
                    res.status(404);
                    res.end();

                }

            }).catch(function (errUpFile) {

                logger.error('[DVP-FIleService.DownloadFile] - [%s] - [PGSQL] - Error occurred while searching Uploaded file  %s',reqId,UUID,errUpFile);
                callback(errUpFile, undefined);
                res.status(400);
                res.end();

            });



        }
        catch (ex) {
            logger.error('[DVP-FIleService.DownloadFile] - [%s] - [FILEDOWNLOAD] - Exception occurred while starting File download service',reqId,UUID);
            callback(new Error("No record Found for the request"), undefined);
            res.status(400);
            res.end();
        }
    }
    else
    {
        logger.error('[DVP-FIleService.DownloadFile] - [%s] - [FILEDOWNLOAD] - Invalid input for UUID %s',reqId,UUID);
        callback(new Error("Invalid input for UUID"), undefined);
        res.status(404);
        res.end();
    }

}

function DownloadLatestFileByID(res,FileName,option,Company,Tenant,reqId)
{

    try {

        logger.debug('[DVP-FIleService.DownloadLatestFileByID] - [%s] - Searching for Uploaded file %s',reqId,FileName);

        DbConn.FileUpload.max('Version',{where: [{Filename: FileName},{CompanyId:Company},{TenantId:Tenant}]}).then(function (resMax) {
            if(resMax)
            {
                logger.debug('[DVP-FIleService.DownloadLatestFileByID] - [%s] - Max version found for file %s',reqId,FileName);

                DbConn.FileUpload.find({where:[{CompanyId:Company},{TenantId:Tenant},{Filename: FileName},{Version:resMax}]}).then(function (resUpFile) {

                    if(resUpFile)
                    {

                        var UUID=resUpFile.UniqueId;
                        logger.debug('[DVP-FIleService.DownloadLatestFileByID] - [%s] - ID found of file %s  ID : %s ',reqId,FileName,UUID);

                        if(option=="MONGO")
                        {

                            logger.debug('[DVP-FIleService.DownloadLatestFileByID] - [%s] - [MONGO] - Downloading from Mongo',reqId,JSON.stringify(resUpFile));

                            var extArr=resUpFile.FileStructure.split('/');
                            var extension=extArr[1];

                            var uri = 'mongodb://'+config.Mongo.user+':'+config.Mongo.password+'@'+config.Mongo.ip+'/'+config.Mongo.dbname;

                            mongodb.MongoClient.connect(uri, function(error, db)
                            {
                                console.log(uri);
                                console.log("Error1 "+error);
                                if(error)
                                {
                                    logger.error('[DVP-FIleService.DownloadLatestFileByID] - [%s] - [MONGO] - Error Connecting Mongo cleint ',reqId);
                                    res.status(400);
                                    res.end();
                                }
                                else
                                {
                                    var bucket = new mongodb.GridFSBucket(db, {
                                        chunkSizeBytes: 1024
                                    });
                                    bucket.openDownloadStreamByName(UUID).
                                        pipe(res).
                                        on('error', function(error) {
                                            console.log('Error !'+error);
                                            res.status(400);
                                            res.end();
                                            //callback(error,undefined);
                                        }).
                                        on('finish', function() {
                                            console.log('done!');
                                            res.status(200);
                                            res.end();
                                            //process.exit(0);
                                        });
                                }

                            });

                        }
                        else if(option=="COUCH")
                        {
                            logger.debug('[DVP-FIleService.DownloadLatestFileByID] - [%s] - [MONGO] - Downloading from Couch',reqId,JSON.stringify(resUpFile));

                            var bucket = cluster.openBucket(Cbucket);

                            bucket.get(UUID, function(err, result) {
                                if (err)
                                {
                                    logger.error('[DVP-FIleService.DownloadLatestFileByID] - [%s] - [MONGO] - Couch Error ',reqId,err);
                                    res.status(400);
                                    res.end();
                                }else
                                {
                                    console.log(resUpFile.FileStructure);
                                    res.setHeader('Content-Type', resUpFile.FileStructure);
                                    var s = streamifier.createReadStream(result.value);
                                    s.pipe(res);


                                    s.on('end', function (result) {
                                        logger.debug('[DVP-FIleService.DownloadLatestFileByID] - [%s] - [FILEDOWNLOAD] - Streaming succeeded',reqId);
                                        SaveDownloadDetails(resUpFile,reqId,function(errSv,resSv)
                                        {
                                            if(errSv)
                                            {
                                                logger.error('[DVP-FIleService.DownloadLatestFileByID] - [%s] - [FILEDOWNLOAD] - Error in Recording downloaded file details',reqId,errSv);

                                            }
                                            else
                                            {
                                                logger.debug('[DVP-FIleService.DownloadLatestFileByID] - [%s] - [FILEDOWNLOAD] - Recording downloaded file details succeeded ',reqId);

                                            }
                                        });


                                        console.log("ENDED");
                                        res.status(200);
                                        res.end();
                                    });
                                    s.on('error', function (err) {
                                        logger.error('[DVP-FIleService.DownloadLatestFileByID] - [%s] - [FILEDOWNLOAD] - Error in streaming',reqId,err);
                                        console.log("ERROR");
                                        res.status(400);
                                        res.end();
                                    });

                                }


                            });
                        }
                        else
                        {
                            logger.debug('[DVP-FIleService.DownloadLatestFileByID] - [%s] - [PGSQL] - Record found for File upload %s',reqId,JSON.stringify(resUpFile));
                            try {
                                res.setHeader('Content-Type', resUpFile.FileStructure);
                                var SourcePath = (resUpFile.URL.toString()).replace('\',' / '');
                                logger.debug('[DVP-FIleService.DownloadLatestFileByID] - [%s]  - [FILEDOWNLOAD] - SourcePath of file %s',reqId,SourcePath);

                                logger.debug('[DVP-FIleService.DownloadLatestFileByID] - [%s]  - [FILEDOWNLOAD] - ReadStream is starting',reqId);
                                var source = fs.createReadStream(SourcePath);

                                source.pipe(res);
                                source.on('end', function (result) {
                                    logger.debug('[DVP-FIleService.DownloadLatestFileByID] - [%s] - [FILEDOWNLOAD] - Piping succeeded',reqId);
                                    res.status(200);
                                    res.end();
                                });
                                source.on('error', function (err) {
                                    logger.error('[DVP-FIleService.DownloadLatestFileByID] - [%s] - [FILEDOWNLOAD] - Error in Piping',reqId,err);
                                    res.status(400);
                                    res.end();
                                });
                            }
                            catch(ex)
                            {
                                logger.error('[DVP-FIleService.DownloadLatestFileByID] - [%s] - [FILEDOWNLOAD] - Exception occurred when download section starts',reqId,ex);
                                res.status(400);
                                res.end();
                            }
                        }
                    }
                    else
                    {
                        logger.error('[DVP-FIleService.DownloadLatestFileByID] - [%s] - No such file found',reqId,FileName);
                        res.status(404);
                        res.end();
                    }

                }).catch(function (errFile) {
                    logger.error('[DVP-FIleService.DownloadLatestFileByID] - [%s] - Error in file searching',reqId,errFile);
                    res.status(400);
                    res.end();
                });
            }
            else
            {
                logger.error('[DVP-FIleService.DownloadLatestFileByID] - [%s] - Max not found',reqId);
                res.status(404);
                res.end();
            }
        }).catch(function (errMax) {
            logger.error('[DVP-FIleService.DownloadLatestFileByID] - [%s] - Error in Max',reqId,errMax);
            res.status(400);
            res.end();
        });

    }
    catch (ex) {
        logger.error('[DVP-FIleService.DownloadLatestFileByID] - [%s] - [FILEDOWNLOAD] - Exception occurred while starting File download service',reqId,FileName);
        res.status(400);
        res.end();
    }


}

function FileInfoByID(res,UUID,Company,Tenant,reqId)
{
    logger.debug('[DVP-FIleService.FileInfoByID] - [%s] - Searching for Uploaded file %s',reqId,UUID);
    if(UUID)
    {
        DbConn.FileUpload.find({where: [{UniqueId: UUID},{CompanyId:Company},{TenantId:Tenant}]}).then(function (resFile) {

            if(resFile)
            {
                res.header('ETag', resFile.UniqueId);
                res.header('Last-Modified', resFile.updatedAt);
                res.status(200);
                res.end();
            }
            else
            {
                logger.debug('[DVP-FIleService.FileInfoByID] - [%s] - No such file found for ID %s',reqId,UUID);
                res.status(404);
                res.end();
            }
        }).catch(function (errFile) {
            logger.error('[DVP-FIleService.FileInfoByID] - [%s] - Error in searching records for ID  %s',reqId,UUID,errFile);
            res.status(400);
            res.end();
        });
    }
    else
    {
        logger.error('[DVP-FIleService.FileInfoByID] - [%s] - Invalid ID  %s',reqId,UUID);
        res.status(404);
        res.end();
    }

};



function LatestFileInfoByID(res,FileName,Company,Tenant,reqId)
{
    try {

        logger.debug('[DVP-FIleService.LatestFileInfoByID] - [%s] - Searching for Uploaded file %s',reqId,FileName);

        DbConn.FileUpload.max('Version',{where: [{Filename: FileName},{CompanyId:Company},{TenantId:Tenant}]}).then(function (resMax) {
            if(resMax)
            {
                DbConn.FileUpload.findOne({where:[{CompanyId:Company},{TenantId:Tenant},{Filename: FileName},{Version:resMax}]}).then(function (resUpFile) {

                    if(resUpFile)
                    {
                        logger.debug('[DVP-FIleService.LatestFileInfoByID] - [%s] - File found FileName %s',reqId,FileName);
                        res.header('ETag', resUpFile.UniqueId);
                        res.header('Last-Modified', resUpFile.updatedAt);
                        res.status(200);
                        res.end();

                    }
                    else
                    {
                        logger.error('[DVP-FIleService.LatestFileInfoByID] - [%s] - File not found FileName %s',reqId,FileName);
                        res.status(404);
                        res.end();
                    }

                }).catch(function (errFile) {
                    logger.error('[DVP-FIleService.LatestFileInfoByID] - [%s] - Error in file searching FileName %s',reqId,FileName,errFile);
                    res.status(400);
                    res.end();
                });
            }
            else
            {
                logger.error('[DVP-FIleService.LatestFileInfoByID] - [%s] - File not found FileName %s',reqId,FileName);
                res.status(404);
                res.end();
            }
        }).catch(function (errMax) {
            logger.error('[DVP-FIleService.LatestFileInfoByID] - [%s] - Error in searching Latest File , FileName %s',reqId,FileName,errMax);
            res.status(400);
            res.end();
        });



    }
    catch (ex) {
        logger.error('[DVP-FIleService.LatestFileInfoByID] - [%s] - [FILEDOWNLOAD] - Exception occurred while starting File download service %s ',reqId,FileName);
        res.status(400);
        res.end();
    }
}


function SaveDownloadDetails(req,reqId,callback)
{

    try {
        var AppObject = DbConn.FileDownload
            .build(
            {
                DownloadId: req.UniqueId,
                ObjClass: req.ObjClass,
                ObjType: req.ObjType,
                ObjCategory: req.ObjCategory,
                DownloadTimestamp: Date.now(),
                Filename: req.Filename,
                CompanyId: req.CompanyId,
                TenantId: req.TenantId


            }
        )
    }
    catch(ex)
    {
        logger.error('[DVP-FIleService.DownloadFile] - [%s] - [FILEDOWNLOAD] - Exception occurred while creating download details',reqId,ex);
        callback(ex, undefined);
    }

    AppObject.save().then(function (resSave) {

        logger.info('[DVP-FIleService.DownloadFile] - [%s] - [PGSQL] - Downloaded file details succeeded ',reqId);
        logger.info('[DVP-FIleService.DownloadFile] - [%s] - [PGSQL] - Downloaded file details succeeded %s',reqId,req.FileStructure);
        callback(undefined, req.FileStructure);

    }).catch(function (errSave) {
        logger.error('[DVP-FIleService.DownloadFile] - [%s] - [PGSQL] - Error occurred while saving download details %s',reqId,JSON.stringify(AppObject),errSave);
        callback(errSave, undefined);
    });

}



module.exports.DownloadFileByID = DownloadFileByID;
module.exports.FileInfoByID = FileInfoByID;
module.exports.DownloadLatestFileByID = DownloadLatestFileByID;
module.exports.LatestFileInfoByID = LatestFileInfoByID;







