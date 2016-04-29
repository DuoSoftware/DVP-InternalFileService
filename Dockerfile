#FROM ubuntu
#RUN apt-get update
#RUN apt-get install -y git nodejs npm nodejs-legacy
#RUN git clone git://github.com/DuoSoftware/DVP-InternalFileService.git /usr/local/src/internalfileservice
#RUN cd /usr/local/src/internalfileservice; npm install
#CMD ["nodejs", "/usr/local/src/internalfileservice/app.js"]

#EXPOSE 8812

FROM node:argon
RUN git clone git://github.com/DuoSoftware/DVP-InternalFileService.git /usr/local/src/internalfileservice
RUN cd /usr/local/src/internalfileservice;
WORKDIR /usr/local/src/internalfileservice
RUN npm install
EXPOSE 8812
CMD [ "node", "/usr/local/src/internalfileservice/app.js" ]