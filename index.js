let fs = require('fs');
let chokidar 	= require('chokidar');
let fsExtra = require('fs-extra');

watch4Files();

function initializeReportGeneration(tivoFilePath) {
    let watchFolder = "/Development/tivo/input-clippy/";
    let archiveFolder = "/Development/tivo/archive-clippy/";
    let errorFolder = "/Development/tivo/error-clippy/";
    let fileName = tivoFilePath.split('/').pop();

  

    console.log(`New file '${fileName}' has been added for processing.`);
    fs.readFile(tivoFilePath, (err, data) => {
        if (err) {
            console.log(`Error with reading data in the file`);
            throw err;
        };
        let tivoRecord;
        
        console.log(`Inside fs.readFile function`);

        try {
            tivoRecord = JSON.parse(data);

            //1. check basic elements/sections.
            let clipMetadata = "clipMetadata";
            let segment = "segment";
            let contentId = "contentId";

            if(clipMetadata in tivoRecord && 
                segment in tivoRecord[clipMetadata][0] && 
                contentId in tivoRecord[clipMetadata][0]){

                let fileNameParts = fileName.split("_");
                let tmsIDPart = fileNameParts[fileNameParts.length - 1].split(".");
                let tmsIDTivoRecord = tmsIDPart[0];

                //1. Identify FileProcessingRequest/TivoTask from loaded 'tivoRecord'
                //2. Check if the mD process is completed successfully (by checking Status in FileProcessingRequest).
                let filter = {TaskID: tmsIDTivoRecord,Status:'CDI_COMPLETE'}
                
                FileProcessingRequest.find(filter).exec().then((cyresRecord)=>{
                    if(cyresRecord == null){
                        TivoRequest.findOneAndUpdate(
                        {TaskID:tmsIDTivoRecord},
                        { $set: { CyresJson: {'comment':'CyresRecord for the respective TivoRecord is null'}, 
                                    TivoJson: tivoRecord,
                                    Report: {'comment':'No report produced as CyresRecord is null'}
                        }}
                        ).exec().then((result) => {
                            fsExtra.move(watchFolder+fileName, errorFolder+fileName, err => {
                                if (err) return console.log(`error while archiving the file ${fileName}`);
                                console.log(`Moved the file ${fileName} to error folder successfully as no respective cyres record is found `);
                            });
                        });
                    } else {
                        let cyresTimeStamp = cyresRecord[0].TimeStamps;
                        let cyresTivoReport = this.loadReport(tivoRecord, cyresTimeStamp, program); // , tmsIDTivoRecord

                        TivoRequest.findOneAndUpdate(
                            {TaskID:tmsIDTivoRecord},
                            {$set: { CyresJson: cyresTimeStamp, 
                                      TivoJson: tivoRecord,
                                      Report: cyresTivoReport
                            }}
                        ).exec().then((result) => {
                            console.log(`Report saved succesfully to the TivoRequest`);
                            fsExtra.move(watchFolder+fileName, archiveFolder+fileName, err => {
                                    if (err) return console.log(`error while archiving the file ${fileName}, error is ${err}`);
                                    console.log(`Moved the file ${fileName} to archive folder successfully `);
                            });
                        }).catch((err)=>{
                            fsExtra.move(watchFolder+fileName, errorFolder+fileName, err => {
                                if (err) return console.log(`error while moving the file ${fileName} to error folder, ${ex}`);
                                console.log(`Moved the error file successfully.`);
                            });
                            console.log(`Error thrown from TivoRequest.findAndModify is ${err}`);
                        });
                    }
                });
            } else {
                console.log(`Problem with contents in the file. Contents in the file don't match minimum requirement(clipMetadata,segments,contentId)`);
                fsExtra.move(watchFolder+fileName, errorFolder+fileName, err => {
                    if (err) return console.log(`error while moving the file ${fileName} to error folder, ${ex}`);
                    console.log(`Moved the error file successfully.`);
                });
                throw "Contents in the file don't match minimum requirement";
            } 
        } catch (ex) {
            fsExtra.move(watchFolder+fileName, errorFolder+fileName, err => {
                if (err) return console.log(`error while moving the file ${fileName} to error folder, ${ex}`);
                console.log(`Moved the error file successfully.`);
            });
            console.log("Error parsing JSON contents:", ex);
            return;
        }
    });
}

function loadReport(tivoJson, cyresJson, program) { // , taskID
    let clipMetadata = tivoJson.clipMetadata[0];
    program.channelAffiliate = clipMetadata.channelAffiliate;
    program.contentTitle     = clipMetadata.contentTitle;
    program.offerStartTime   = clipMetadata.offerStartTime;

    let cyresJsonObject = JSON.parse(cyresJson);
    let tivoSegmentArray  = clipMetadata.segment;
    let cyresSegmentArrayAll = cyresJsonObject.SEGMENTLIST;

    let  cyresProgramSegmentArray =  cyresSegmentArrayAll.filter((segment)=>{
                                return segment.SegmentType == "Program Segment";
                              })

    let resultsSegmentArray = [];
    let commentsArray = [];

    //check if the #of segments are same.
    if(cyresProgramSegmentArray.length !== tivoSegmentArray.length){
        if(Math.abs(cyresProgramSegmentArray.length - tivoSegmentArray.length) == 1){
            program.status = "warning";
            console.log(`Number of segments not matching, cyres segments are ${cyresProgramSegmentArray.length} and tivo segments are ${tivoSegmentArray.length}`);
            commentsArray.push(
                    {'comment':`Number of segments not matching cyres segments are ${cyresProgramSegmentArray.length} and tivo segments are ${tivoSegmentArray.length}`
                });
            if(cyresProgramSegmentArray.length - tivoSegmentArray.length == 1){

                //When Cyres found 1 additional segment than Tivo. Ignore the last segment on Cyres.
                tivoSegmentArray.forEach((tivoSegment,index) => {
                    let cyresDuration = (cyresProgramSegmentArray[index].OutPointFrameBoundaryEndSecs - cyresProgramSegmentArray[index].InPointFrameBoundaryStartSecs)*1000;
                    let tivoDuration  =  (tivoSegment.endOffset - tivoSegment.startOffset); 
                    
                    //Check if the difference in durations is more than 30.
                    if(Math.abs((cyresDuration-tivoDuration)/1000) < 30){
                        console.log(`Gap less than 30`);
                        resultsSegmentArray.push({'cyresDuration':Math.round(cyresDuration),
                                                'tivoDuration':Math.round(tivoDuration),
                                                'difference(cyres - tivo)': Math.round(cyresDuration - tivoDuration)
                                                })
                    } else {
                        console.log(`Gap greater than or == 30`);
                        console.log(`The gap between the segment number ${index+1} in Tivo and Cyres JSON is greater than 30 sec`);
                        commentsArray.push({'comment':`The gap between segment number ${index+1} in Tivo and Cyres JSON is greater than 30 sec`})
                    }
                });
            }
        }
        else if(Math.abs(cyresProgramSegmentArray.length - tivoSegmentArray.length) > 1){
            program.status = "error"
            console.log(`Number of segments mismatch is huge, cyres segments are ${cyresProgramSegmentArray.length} and tivo segments are ${tivoSegmentArray.length}`);
            commentsArray.push({'comment':`cyres segments are ${cyresProgramSegmentArray.length} and tivo segments are ${tivoSegmentArray.length}. Differnce is greater than one. Cannot procees further`
            })
        }
    } 
    else {
        cyresProgramSegmentArray.forEach((cyresSegment,index) => {
            let cyresDuration = (cyresSegment.OutPointFrameBoundaryEndSecs - cyresSegment.InPointFrameBoundaryStartSecs)*1000;
            let tivoDuration  =  tivoSegmentArray[index].endOffset - tivoSegmentArray[index].startOffset; 
            program.status = "completed";
            resultsSegmentArray.push({'cyresDuration':Math.round(cyresDuration),
                                'tivoDuration':Math.round(tivoDuration),
                                'difference(cyres - tivo)': Math.round(cyresDuration - tivoDuration),
                              })
        });
    }

    program.segmentMap = resultsSegmentArray;
    program.comments   = commentsArray;

    return program;
}

function watch4Files() {
  console.log(`Checking for any new file in the watch folder`);

        let watchFolder = "/Development/tivo/input-clippy";
        let watcher = chokidar.watch(watchFolder, {
    persistent: true,
    cwd: '.',
    usePolling: true,
    interval: 100,
    ignorePermissionErrors: false,
    atomic: true // or a custom 'atomicity delay', in milliseconds (default 100)
  });

  watcher.on('ready', () => console.log('FileWatcher inititalized, Ready to watch folder:' + watchFolder));

  watcher.on('add', tivoFilePath => {
            this.initializeReportGeneration(tivoFilePath);
        })
        .on('change', tivoFilePath => console.log(`File ${tivoFilePath} has been changed`))
  .on('unlink', tivoFilePath => console.log(`File ${tivoFilePath} has been removed`));
}
