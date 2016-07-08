/**
 * Created by pawan on 2/23/2015.
 */

var restify = require('restify');
var uuid = require('node-uuid');
var config = require('config');


var FileHandler=require('./FileHandlerApi.js');
var RedisPublisher=require('./RedisPublisher.js');
var DeveloperFileUpoladManager = require('./DeveloperFileUpoladManager.js');
var messageFormatter = require('dvp-common/CommonMessageGenerator/ClientMessageJsonFormatter.js');
var logger = require('dvp-common/LogHandler/CommonLogHandler.js').logger;



var port = config.Host.port || 3000;
var version=config.Host.version;
var option = config.Option;




var RestServer = restify.createServer({
    name: "myapp",
    version: '1.0.0'
},function(req,res)
{

});


RestServer.use(restify.CORS());
RestServer.use(restify.fullResponse());
RestServer.pre(restify.pre.userAgentConnection());


//Server listen
RestServer.listen(port, function () {
    console.log('%s listening at %s', RestServer.name, RestServer.url);


});
//Enable request body parsing(access)
RestServer.use(restify.bodyParser());
RestServer.use(restify.acceptParser(RestServer.acceptable));
RestServer.use(restify.queryParser());




RestServer.get('/DVP/API/'+version+'/FileService/File/Download/:tenant/:company/:id/:displayname',function(req,res,next)
{
    var reqId='';

    try {

        try
        {
            reqId = uuid.v1();
        }
        catch(ex)
        {

        }

        logger.debug('[DVP-FIleService.DownloadFile] - [%s] - [HTTP] - Request received - Inputs - File ID : %s ',reqId,req.params.id);

        var Company=req.params.company;
        var Tenant=req.params.tenant;


        FileHandler.DownloadFileByID(res,req.params.id,req.params.displayname,option,Company,Tenant,reqId,function(errDownFile,resDownFile)
        {
            if(errDownFile)
            {
                var jsonString = messageFormatter.FormatMessage(errDownFile, "ERROR/EXCEPTION", false, undefined);
                logger.debug('[DVP-FIleService.DownloadFile] - [%s] - Request response : %s ', reqId, jsonString);


            }
            else
            {
                var jsonString = messageFormatter.FormatMessage(undefined, "SUCCESS", true, resDownFile);
                logger.debug('[DVP-FIleService.DownloadFile] - [%s] - Request response : %s ', reqId, jsonString);


            }

        });



    }
    catch(ex)
    {
        logger.error('[DVP-FIleService.DownloadFile] - [%s] - [HTTP] - Error in Request - Inputs - File ID : %s ',reqId,req.params.id,ex);
        var jsonString = messageFormatter.FormatMessage(ex, "EXCEPTION", false, undefined);
        res.end(jsonString);
    }

    return next();

});

// for freeswitch compatability
RestServer.head('/DVP/API/'+version+'/FileService/File/Download/:tenant/:company/:id/:displayname',function(req,res,next)
{
    var reqId='';
    try {

        try
        {
            reqId = uuid.v1();
        }
        catch(ex)
        {

        }

        logger.debug('[DVP-FIleService.DownloadFile] - [%s] - [HTTP] - Request received - Inputs - File ID : %s ',reqId,req.params.id);

        var Company=req.params.company;
        var Tenant=req.params.tenant;


        FileHandler.FileInfoByID(res,req.params.id,Company,Tenant,reqId);




    }
    catch(ex)
    {
        logger.error('[DVP-FIleService.DownloadFile] - [%s] - [HTTP] - Error in Request - Inputs - File ID : %s ',reqId,req.params.id,ex);
        var jsonString = messageFormatter.FormatMessage(ex, "EXCEPTION", false, undefined);
        res.status(400);
        res.end(jsonString);
    }

    return next();

});

RestServer.get('/DVP/API/'+version+'/FileService/File/DownloadLatest/:tenant/:company/:filename',function(req,res,next)
{
    var reqId='';

    try {

        try
        {
            reqId = uuid.v1();
        }
        catch(ex)
        {

        }

        //logger.debug('[DVP-FIleService.DownloadFile] - [%s] - [HTTP] - Request received - Inputs - File ID : %s ',reqId,req.params.id);

        var Company=req.params.company;
        var Tenant=req.params.tenant;


        FileHandler.DownloadLatestFileByID(res,req.params.filename,option,Company,Tenant,reqId);




    }
    catch(ex)
    {
        // logger.error('[DVP-FIleService.DownloadFile] - [%s] - [HTTP] - Error in Request - Inputs - File ID : %s ',reqId,req.params.id,ex);
        var jsonString = messageFormatter.FormatMessage(ex, "EXCEPTION", false, undefined);
        res.status(404);
        res.end(jsonString);
    }

    return next();

});

RestServer.head('/DVP/API/'+version+'/FileService/File/DownloadLatest/:tenant/:company/:filename',function(req,res,next)
{
    var reqId='';
    try {

        try
        {
            reqId = uuid.v1();
        }
        catch(ex)
        {

        }

        //logger.debug('[DVP-FIleService.DownloadFile] - [%s] - [HTTP] - Request received - Inputs - File ID : %s ',reqId,req.params.id);

        var Company=req.params.company;
        var Tenant=req.params.tenant;


        FileHandler.LatestFileInfoByID(res,req.params.filename,Company,Tenant,reqId);


    }
    catch(ex)
    {
        //logger.error('[DVP-FIleService.DownloadFile] - [%s] - [HTTP] - Error in Request - Inputs - File ID : %s ',reqId,req.params.id,ex);
        var jsonString = messageFormatter.FormatMessage(ex, "EXCEPTION", false, undefined);
        res.status(400);
        res.end(jsonString);
    }

    return next();

});

RestServer.put('/DVP/API/'+version+'/FileService/File/Upload/:tenant/:company',function(req,res,next)
{

    var reqId='';
    try
    {
        reqId = uuid.v1();
    }
    catch(ex)
    {

    }



    var Company=req.params.company;
    var Tenant=req.params.tenant;


    var prov=1;

    var Clz='';
    var Type='';
    var Category="";
    var ref="";
    var FileStructure="";
    var FilePath="";
    var FileName="";
    var BodyObj="";
    var DisplayName="";


    req.readable=true;

console.log("params "+req.params);
console.log("body "+req.body);
console.log("query "+req.query);




    if (req.params)
    {
        if (req.params.class) {
            Clz = req.params.class;

        }
        if (req.params.type) {

            Type = req.params.type;
        }
        if (req.params.category) {
            Category = req.params.category;

        }
        if (req.params.referenceid) {
            ref = req.params.referenceid;
        }
        if (req.params.fileCategory) {
            Category = req.params.fileCategory;

        }
    }

    if(req.query)
    {
        if(req.query.put_file)
        {
            FilePath=req.query.put_file;
        }

        if(req.query.class)
        {
            Clz=req.query.class;
        }
        if(req.query.type)
        {
            Type=req.query.type;
        }
        if(req.query.category)
        {
            Category=req.query.category;
        }
        if(req.query.sessionid)
        {
            ref=req.query.sessionid;
        }
        if(req.query.mediatype && req.query.filetype)
        {
            if(req.query.filetype=="wav" || req.query.filetype=="mp3")
            {
                FileStructure="audio/"+req.query.filetype;
            }
            else
            {
                FileStructure=req.query.mediatype+"/"+req.query.filetype;
            }

        }
        if(req.query.sessionid && req.query.filetype)
        {
            FileName=req.query.sessionid+"."+req.query.filetype;
        }
        if(req.query.display)
        {
            DisplayName=req.query.display;
        }



    }

    if(req.body)
    {
        if (req.body.class) {
            Clz = req.body.class;

        }
        if (req.body.fileCategory) {
            Category = req.body.fileCategory;

        }
        if (req.body.category) {
            Category = req.body.category;

        }

        if (req.body.type) {

            Type = req.body.type;
        }
        if (req.body.referenceid) {
            ref = req.body.referenceid;
        }
        if(req.body.display)
        {
            DisplayName=req.body.display;
        }

        BodyObj=req.body;
    }




    try {


        logger.debug('[DVP-FIleService.UploadFiles] - [%s] - [HTTP] - Request received - Inputs - Provision : %s Company : %s Tenant : %s',reqId,prov,Company,Tenant);

        var rand2 = uuid.v4().toString();
        /* var fileKey = Object.keys(req.files)[0];
         var file = req.files[fileKey];*/

        logger.info('[DVP-FIleService.UploadFiles] - [%s] - [FILEUPLOAD] - File path %s ',reqId,FilePath);


        var ValObj={

            "tenent":Tenant,
            "company":Company,
            "filename":FileName,
            "type":Type,
            "id":rand2

        };

        var file=
        {
            fClass:Clz,
            type:Type,
            fCategory:Category,
            fRefID:ref,
            fStructure:FileStructure,
            path:FilePath,
            name:FileName,
            displayname:DisplayName
        };


        var AttchVal=JSON.stringify(ValObj);


        logger.debug('[DVP-FIleService.UploadFiles] - [%s] - [FILEUPLOAD] - Attachment values %s',reqId,AttchVal);


        //DeveloperFileUpoladManager.InternalUploadFiles(file,rand2,Company, Tenant,option,req,reqId,function (errz, respg)
        DeveloperFileUpoladManager.InternalUploadFiles(file,rand2,Company, Tenant,option,req,reqId,function (errz, respg)

        {


            if(errz)
            {
                var jsonString = messageFormatter.FormatMessage(errz, "ERROR/EXCEPTION", false, undefined);
                logger.debug('[DVP-FIleService.UploadFiles] - [%s] - Request response : %s ', reqId, jsonString);
                res.end(jsonString);
            }

            else{


                logger.debug('[DVP-FIleService.UploadFiles] - [%s] - To publishing on redis - ServerID  %s Attachment values : %s',reqId,JSON.stringify(respg),AttchVal);
                RedisPublisher.RedisPublish(respg, AttchVal,reqId, function (errRDS, resRDS) {
                        if (errRDS)
                        {
                            var jsonString = messageFormatter.FormatMessage(errRDS, "ERROR/EXCEPTION", false, undefined);
                            logger.debug('[DVP-FIleService.UploadFiles] - [%s] - Request response : %s ', reqId, jsonString);
                            res.end(jsonString);



                        }
                        else
                        {
                            var jsonString = messageFormatter.FormatMessage(undefined, "SUCCESS", true, rand2);
                            logger.debug('[DVP-FIleService.UploadFiles] - [%s] - Request response : %s ', reqId, jsonString);
                            res.end(jsonString);

                        }


                    }
                );


            }



        });


    }
    catch(ex)
    {
        var x = JSON.parse(req);
        console.log(JSON.stringify(x));
        logger.error('[DVP-FIleService.UploadFiles] - [%s] - [HTTP] - Exception occurred when Developer file upload request starts  ',reqId);
        var jsonString = messageFormatter.FormatMessage(ex, "EXCEPTION", false, undefined);
        logger.debug('[DVP-FIleService.UploadFiles] - [%s] - Request response : %s ', reqId, jsonString);
        res.end(JSON.stringify(x));
    }
    return next();
});

RestServer.post('/DVP/API/'+version+'/FileService/File/Upload/:tenant/:company',function(req,res,next)
{

    var reqId='';
    try
    {
        reqId = uuid.v1();
    }
    catch(ex)
    {

    }



    var Company=req.params.company;
    var Tenant=req.params.tenant;


    var prov=1;

    var Clz='';
    var Type='';
    var Category="";
    var ref="";
    var FileStructure="";
    var FilePath="";
    var FileName="";
    var BodyObj="";
    var DisplayName="";
    var Display="";


    req.readable=true;



    /* if(req.query)
     {
     /!* FilePath=req.query.put_file;*!/
     Clz=req.query.class;
     Type=req.query.type;
     Category=req.query.category;
     ref=req.query.referenceid;
     /!*FileStructure=req.query.mediatype+"/"+req.query.filetype;
     FileName=req.query.sessionid+"."+req.query.filetype;
     DisplayName=req.query.display;*!/
     }

     if(req.body)
     {
     console.log("Body Found");
     BodyObj=req.body;
     Clz=req.body.class;
     Type=req.query.type;
     Category=req.query.category;
     ref=req.query.referenceid;
     }
     */


    if (req.body) {
        if (req.body.class) {
            Clz = req.body.class;

        }
        if (req.body.fileCategory) {
            Category = req.body.fileCategory;

        }
        if (req.body.category) {
            Category = req.body.category;

        }

        if (req.body.type) {

            Type = req.body.type;
        }
        if (req.body.referenceid) {
            ref = req.body.referenceid;
        }
        if(req.body.display)
        {
            Display=req.body.display;
        }
    }


    if (req.params) {
        if (req.params.class) {
            Clz = req.params.class;

        }
        if (req.params.type) {

            Type = req.params.type;
        }
        if (req.params.category) {
            Category = req.params.category;

        }
        if (req.params.referenceid) {
            ref = req.params.referenceid;
        }
        if (req.params.fileCategory) {
            Category = req.params.fileCategory;

        }
    }




    try {


        logger.debug('[DVP-FIleService.UploadFiles] - [%s] - [HTTP] - Request received - Inputs - Provision : %s Company : %s Tenant : %s',reqId,prov,Company,Tenant);

        var rand2 = uuid.v4().toString();
        var fileKey = Object.keys(req.files)[0];
        var attachedFile = req.files[fileKey];
        FileStructure=attachedFile.type;
        FileName=attachedFile.name;
        FilePath=attachedFile.path;

        var DisplyArr = attachedFile.path.split('\\');

        var DisplayName=DisplyArr[DisplyArr.length-1];





        logger.info('[DVP-FIleService.UploadFiles] - [%s] - [FILEUPLOAD] - File path %s ',reqId,FilePath);


        var ValObj={

            "tenent":Tenant,
            "company":Company,
            "filename":FileName,
            "type":Type,
            "id":rand2

        };

        var file=
        {
            fClass:Clz,
            type:Type,
            fCategory:Category,
            fRefID:ref,
            fStructure:FileStructure,
            path:FilePath,
            name:FileName,
            displayname:Display
        };

        console.log("File Data "+file);


        var AttchVal=JSON.stringify(ValObj);


        logger.debug('[DVP-FIleService.UploadFiles] - [%s] - [FILEUPLOAD] - Attachment values %s',reqId,AttchVal);


        //DeveloperFileUpoladManager.InternalUploadFiles(file,rand2,Company, Tenant,option,req,reqId,function (errz, respg)
        DeveloperFileUpoladManager.InternalUploadFiles(file,rand2,Company, Tenant,option,req,reqId,function (errz, respg)

        {


            if(errz)
            {
                var jsonString = messageFormatter.FormatMessage(errz, "ERROR/EXCEPTION", false, undefined);
                logger.debug('[DVP-FIleService.UploadFiles] - [%s] - Request response : %s ', reqId, jsonString);
                res.end(jsonString);
            }

            else{


                logger.debug('[DVP-FIleService.UploadFiles] - [%s] - To publishing on redis - ServerID  %s Attachment values : %s',reqId,JSON.stringify(respg),AttchVal);
                RedisPublisher.RedisPublish(respg, AttchVal,reqId, function (errRDS, resRDS) {
                        if (errRDS)
                        {
                            var jsonString = messageFormatter.FormatMessage(errRDS, "ERROR/EXCEPTION", false, undefined);
                            logger.debug('[DVP-FIleService.UploadFiles] - [%s] - Request response : %s ', reqId, jsonString);
                            res.end(jsonString);



                        }
                        else
                        {
                            var jsonString = messageFormatter.FormatMessage(undefined, "SUCCESS", true, rand2);
                            logger.debug('[DVP-FIleService.UploadFiles] - [%s] - Request response : %s ', reqId, jsonString);
                            res.end(jsonString);

                        }


                    }
                );


            }



        });


    }
    catch(ex)
    {
        var x = JSON.parse(req);
        console.log(JSON.stringify(x));
        logger.error('[DVP-FIleService.UploadFiles] - [%s] - [HTTP] - Exception occurred when Developer file upload request starts  ',reqId);
        var jsonString = messageFormatter.FormatMessage(ex, "EXCEPTION", false, undefined);
        logger.debug('[DVP-FIleService.UploadFiles] - [%s] - Request response : %s ', reqId, jsonString);
        res.end(JSON.stringify(x));
    }
    return next();
});


function Crossdomain(req,res,next){


    var xml='<?xml version=""1.0""?><!DOCTYPE cross-domain-policy SYSTEM ""http://www.macromedia.com/xml/dtds/cross-domain-policy.dtd""> <cross-domain-policy>    <allow-access-from domain=""*"" />        </cross-domain-policy>';

    var xml='<?xml version="1.0"?>\n';

    xml+= '<!DOCTYPE cross-domain-policy SYSTEM "/xml/dtds/cross-domain-policy.dtd">\n';
    xml+='';
    xml+=' \n';
    xml+='\n';
    xml+='';
    req.setEncoding('utf8');
    res.end(xml);

}

function Clientaccesspolicy(req,res,next){


    var xml='<?xml version="1.0" encoding="utf-8" ?>       <access-policy>        <cross-domain-access>        <policy>        <allow-from http-request-headers="*">        <domain uri="*"/>        </allow-from>        <grant-to>        <resource include-subpaths="true" path="/"/>        </grant-to>        </policy>        </cross-domain-access>        </access-policy>';
    req.setEncoding('utf8');
    res.end(xml);

}

RestServer.get("/crossdomain.xml",Crossdomain);
RestServer.get("/clientaccesspolicy.xml",Clientaccesspolicy);



