/**
 * Created by pawan on 4/9/2015.
 */
//.....................................................................................................
// change mongodb module to mongoose
//.....................................................................................................

var DbConn = require('dvp-dbmodels');
var fs=require('fs');
var config = require('config');
var mongodb = require('mongodb');
var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;



function DeveloperUploadFiles(Fobj,rand2,cmp,ten,ref,option,Clz,Type,Category,reqId,callback)
{

    try
    {

        var DisplyArr = Fobj.path.split('\\');

        var DisplayName=DisplyArr[DisplyArr.length-1];

        FindCurrentVersion(Fobj,cmp,ten,reqId,function(err,result)
        {
            if(err)
            {
                callback(err,undefined);
            }
            else
            {
                if(option=="LOCAL")
                {
                    logger.info('[DVP-FIleService.DeveloperUploadFiles] - [%s] - [PGSQL] - New attachment  successfully inserted to Local',reqId);
                    //callback(undefined, resUpFile.UniqueId);
                    FileUploadDataRecorder(Fobj,rand2,cmp,ten,ref,Clz,Type,Category,result,DisplayName, function (err,res) {
                        callback(err,rand2);
                    });
                }
                else if(option=="MONGO")
                {
                    logger.info('[DVP-FIleService.DeveloperUploadFiles] - [%s]  - New attachment on process of uploading to MongoDB',reqId);
                    console.log("TO MONGO >>>>>>>>> "+rand2);
                    MongoUploader(rand2,Fobj.path,reqId,function(errMongo,resMongo)
                    {
                        if(errMongo)
                        {
                            console.log(errMongo);
                            callback(errMongo,undefined);
                        }
                        else
                        {
                            console.log(resMongo);
                            // callback(undefined,resUpFile.UniqueId);
                            FileUploadDataRecorder(Fobj,rand2,cmp,ten,ref,Clz,Type,Category,result,DisplayName , function (err,res) {
                                if(err)
                                {
                                    callback(err,undefined);

                                }
                                else
                                {
                                    if(res)
                                    {
                                        callback(undefined,res);
                                    }
                                    else
                                    {
                                        callback(new Error("Error in Operation "),undefined);
                                    }
                                }
                            });
                        }



                    });
                }
                // sprint 5
                /*else if(option=="COUCH")
                 {
                 logger.info('[DVP-FIleService.DeveloperUploadFiles] - [%s]  - New attachment object %s on process of uploading to COUCH',reqId);
                 console.log("TOCOUCH >>>>>>>>> "+rand2);
                 CouchUploader(rand2,Fobj,resUpFile,reqId,function(errCouch,resCouch)
                 {
                 if(errCouch)
                 {
                 console.log(errCouch);
                 callback(errCouch,undefined);
                 }
                 else
                 {
                 console.log(resCouch);
                 FileUploadDataRecorder(Fobj,rand2,cmp,ten,ref,Clz,Type,Category,result, function (err,res) {
                 callback(err,res.UniqueId);
                 });
                 }

                 });

                 }*/

            }
        });

    }
    catch(ex)
    {
        logger.error('[DVP-FIleService.DeveloperUploadFiles] - [%s] - Exception occurred when new attachment object saving starting ',reqId,ex);
        callback(ex,undefined);
    }






}

function FindCurrentVersion(FObj,company,tenant,reqId,callback)
{
    try
    {
        logger.debug('[DVP-FIleService.FindCurrentVersion.FindCurrentVersion] - [%s] - Searching for current version of %s',reqId,FObj.name);
        DbConn.FileUpload.max('Version',{where: [{Filename: FObj.name},{CompanyId:company},{TenantId:tenant}]}).then(function (resFile) {

            if(resFile)
            {
                logger.debug('[DVP-FIleService.FindCurrentVersion.FindCurrentVersion] - [%s] - [PGSQL] - Old version of % is found and New version will be %d',reqId,FObj.name,parseInt((resFile)+1));
                callback(undefined,parseInt((resFile)+1));
            }
            else
            {
                logger.debug('[DVP-FIleService.FindCurrentVersion.FindCurrentVersion] - [%s] - [PGSQL] -  Version of %s is not found and New version will be 1',reqId,FObj.name);
                callback(undefined,1);
            }

        }).catch(function (errFile) {

            logger.error('[DVP-FIleService.FindCurrentVersion.FindCurrentVersion] - [%s] - [PGSQL] - Error occurred while searching for current version of %s',reqId,FObj.name,errFile);
            callback(errFile,undefined);

        });

    }
    catch (ex)
    {
        logger.error('[DVP-FIleService.FindCurrentVersion.FindCurrentVersion] - [%s] - Exception occurred when start searching current version of %s',reqId,FObj.name,ex);
        callback(ex,undefined);
    }
}

function FileUploadDataRecorder(Fobj,rand2,cmp,ten,result,callback )
{
    try
    {
        var NewUploadObj = DbConn.FileUpload
            .build(
            {
                UniqueId: rand2,
                FileStructure: Fobj.fStructure,
                ObjClass: Fobj.fClass,
                ObjType: Fobj.type,
                ObjCategory: Fobj.fCategory,
                URL: Fobj.path,
                UploadTimestamp: Date.now(),
                Filename: Fobj.name,
                Version:result,
                DisplayName: Fobj.displayname,
                CompanyId:cmp,
                TenantId: ten,
                RefId:Fobj.fRefID


            }
        );
        //logger.debug('[DVP-FIleService.DeveloperUploadFiles] - [%s] - New attachment object %s',reqId,JSON.stringify(NewUploadObj));
        NewUploadObj.save().then(function (resUpFile) {

            //logger.info('[DVP-FIleService.DeveloperUploadFiles] - [%s] - [PGSQL] - New attachment object %s successfully inserted',reqId,JSON.stringify(NewUploadObj));
            if(resUpFile)
            {
                DbConn.FileCategory.find({where:{Category:Fobj.fCategory}}).then(function (resCat) {

                    if(resCat)
                    {
                        resUpFile.setFileCategory(resCat.id).then(function (resCatset) {

                            callback(undefined,resUpFile.UniqueId);

                        }).catch(function (errCatSet) {
                            callback(errCatSet,undefined);
                        });
                    }
                    else
                    {
                        callback(undefined,resUpFile.UniqueId)
                    }



                }).catch(function (errCat) {
                    callback(errCat,undefined);
                });
            }
            else
            {
                callback(new Error("Upload records saving failed"),undefined);
            }




        }).catch(function (errUpFile) {

            //logger.error('[DVP-FIleService.DeveloperUploadFiles] - [%s] - [PGSQL] - New attachment object %s insertion failed',reqId,JSON.stringify(NewUploadObj),errUpFile);
            callback(errUpFile, undefined);



        });

    }
    catch(ex)
    {
        //logger.error('[DVP-FIleService.DeveloperUploadFiles] - [%s] - Exception occurred when new attachment object creating ',reqId,ex);
        callback(ex,undefined);
    }
}

function MongoUploader(uuid,path,reqId,callback)
{


    try {
        var uri = 'mongodb://' + config.Mongo.user + ':' + config.Mongo.password + '@' + config.Mongo.ip + '/' + config.Mongo.dbname;
        mongodb.MongoClient.connect(uri, function (error, db) {
            console.log(uri);
            console.log("Error1 " + error);
            //console.log("db "+JSON.stringify(db));
            //assert.ifError(error);
            var bucket = new mongodb.GridFSBucket(db);

            fs.createReadStream(path).pipe(bucket.openUploadStream(uuid)).
                on('error', function (error) {
                    // assert.ifError(error);
                    console.log("Error " + error);
                    callback(error, undefined);
                }).
                on('finish', function () {
                    console.log('done!');
                    //process.exit(0);
                    callback(undefined, uuid);
                });

        });
    } catch (e) {
        callback(e, undefined);
    }

}

/*function DeveloperUploadFiles(Fobj,rand2,cmp,ten,ref,option,Clz,Type,Category,reqId,callback)
 {

 try
 {
 var DisplyArr = Fobj.path.split('\\');

 var DisplayName=DisplyArr[DisplyArr.length-1];
 }
 catch(ex)
 {
 logger.error('[DVP-FIleService.DeveloperUploadFiles] - [%s] - Exception occurred while creating DisplayName %s',reqId,JSON.stringify(Fobj));
 callback(ex,undefined);
 }

 try
 {

 FindCurrentVersion(Fobj,cmp,ten,reqId,function(err,result)
 {
 if(err)
 {
 callback(err,undefined);
 }
 else
 {
 try
 {
 var NewUploadObj = DbConn.FileUpload
 .build(
 {
 UniqueId: rand2,
 FileStructure: Fobj.type,
 ObjClass: Clz,
 ObjType: Type,
 ObjCategory: Category,
 URL: Fobj.path,
 UploadTimestamp: Date.now(),
 Filename: Fobj.name,
 Version:result,
 DisplayName: DisplayName,
 CompanyId:cmp,
 TenantId: ten,
 RefId:ref


 }
 );
 logger.debug('[DVP-FIleService.DeveloperUploadFiles] - [%s] - New attachment object %s',reqId,JSON.stringify(NewUploadObj));
 NewUploadObj.save().then(function (resUpFile) {

 logger.info('[DVP-FIleService.DeveloperUploadFiles] - [%s] - [PGSQL] - New attachment object %s successfully inserted',reqId,JSON.stringify(NewUploadObj));

 DbConn.FileCategory.find({where:{Category:Category}}).then(function (resCat) {

 resUpFile.setFileCategory(resCat.id).then(function (resCatset) {

 console.log("Category attached successfully");

 }).catch(function (errCatSet) {
 console.log("Error in category attaching "+errCatSet);
 });

 }).catch(function (errCat) {
 console.log("Error in searching file categories "+errCat);
 });

 if(option=="LOCAL")
 {
 logger.info('[DVP-FIleService.DeveloperUploadFiles] - [%s] - [PGSQL] - New attachment object %s successfully inserted to Local',reqId,JSON.stringify(NewUploadObj));
 callback(undefined, resUpFile.UniqueId);
 }
 else if(option=="MONGO")
 {
 logger.info('[DVP-FIleService.DeveloperUploadFiles] - [%s]  - New attachment object %s on process of uploading to MongoDB',reqId,JSON.stringify(NewUploadObj));
 console.log("TO MONGO >>>>>>>>> "+rand2);
 MongoUploader(rand2,Fobj.path,reqId,function(errMongo,resMongo)
 {
 if(errMongo)
 {
 console.log(errMongo);
 callback(errMongo,undefined);
 }else
 {
 console.log(resMongo);
 callback(undefined,resUpFile.UniqueId);
 }

 });
 }
 // sprint 5
 else if(option=="COUCH")
 {
 logger.info('[DVP-FIleService.DeveloperUploadFiles] - [%s]  - New attachment object %s on process of uploading to COUCH',reqId,JSON.stringify(NewUploadObj));
 console.log("TOCOUCH >>>>>>>>> "+rand2);
 CouchUploader(rand2,Fobj,resUpFile,reqId,function(errCouch,resCouch)
 {
 if(errCouch)
 {
 console.log(errCouch);
 callback(errCouch,undefined);
 }
 else
 {
 console.log(resCouch);
 callback(undefined,resUpFile.UniqueId);
 }

 });

 }




 }).catch(function (errUpFile) {

 logger.error('[DVP-FIleService.DeveloperUploadFiles] - [%s] - [PGSQL] - New attachment object %s insertion failed',reqId,JSON.stringify(NewUploadObj),errUpFile);
 callback(errUpFile, undefined);



 });

 }
 catch(ex)
 {
 logger.error('[DVP-FIleService.DeveloperUploadFiles] - [%s] - Exception occurred when new attachment object creating ',reqId,ex);
 callback(ex,undefined);
 }
 }
 });

 }
 catch(ex)
 {
 logger.error('[DVP-FIleService.DeveloperUploadFiles] - [%s] - Exception occurred when new attachment object saving starting ',reqId,ex);
 callback(ex,undefined);
 }






 }*/


function InternalUploadFiles(Fobj,rand2,cmp,ten,option,BodyObj,reqId,callback)
{

    try
    {



        var DisplayName=Fobj.name;

        FindCurrentVersion(Fobj,cmp,ten,reqId,function(err,result)
        {
            if(err)
            {
                callback(err,undefined);
            }
            else
            {
                if(option=="LOCAL")
                {
                    logger.info('[DVP-FIleService.DeveloperUploadFiles] - [%s] - [PGSQL] - New attachment  successfully inserted to Local',reqId);
                    //callback(undefined, resUpFile.UniqueId);
                    FileUploadDataRecorder(Fobj,rand2,cmp,ten,result, function (err,res) {
                        callback(err,rand2);
                    });
                }
                else if(option=="MONGO")
                {
                    logger.info('[DVP-FIleService.DeveloperUploadFiles] - [%s]  - New attachment on process of uploading to MongoDB',reqId);
                    console.log("TO MONGO >>>>>>>>> "+rand2);
                    MongoUploader(rand2,BodyObj,reqId,function(errMongo,resMongo)
                    {
                        if(errMongo)
                        {
                            console.log(errMongo);
                            callback(errMongo,undefined);
                        }
                        else
                        {
                            console.log(resMongo);
                            // callback(undefined,resUpFile.UniqueId);
                            FileUploadDataRecorder(Fobj,rand2,cmp,ten,result, function (err,res) {
                                if(err)
                                {
                                    callback(err,undefined);

                                }
                                else
                                {
                                    if(res)
                                    {
                                        callback(undefined,res);
                                    }
                                    else
                                    {
                                        callback(new Error("Error in Operation "),undefined);
                                    }
                                }
                            });
                        }



                    });
                }
                // sprint 5
                /*else if(option=="COUCH")
                 {
                 logger.info('[DVP-FIleService.DeveloperUploadFiles] - [%s]  - New attachment object %s on process of uploading to COUCH',reqId);
                 console.log("TOCOUCH >>>>>>>>> "+rand2);
                 CouchUploader(rand2,Fobj,resUpFile,reqId,function(errCouch,resCouch)
                 {
                 if(errCouch)
                 {
                 console.log(errCouch);
                 callback(errCouch,undefined);
                 }
                 else
                 {
                 console.log(resCouch);
                 FileUploadDataRecorder(Fobj,rand2,cmp,ten,ref,Clz,Type,Category,result, function (err,res) {
                 callback(err,res.UniqueId);
                 });
                 }

                 });

                 }*/

            }
        });

    }
    catch(ex)
    {
        logger.error('[DVP-FIleService.DeveloperUploadFiles] - [%s] - Exception occurred when new attachment object saving starting ',reqId,ex);
        callback(ex,undefined);
    }






}


module.exports.DeveloperUploadFiles = DeveloperUploadFiles;
module.exports.InternalUploadFiles = InternalUploadFiles;



