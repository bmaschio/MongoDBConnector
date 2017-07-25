
include "./public/interfaces/MongoDbConnector.iol"
include "string_utils.iol"
include "time.iol"
include "file.iol"
include "console.iol"

init{

    connectValue.host = "10.101.50.107";
    connectValue.dbname ="ClientData";
    connectValue.port = 27017;
    connectValue.jsonStringDebug = false;
    connectValue.timeZone = "Europe/Berlin";
    connectValue.username = "prova";
    connectValue.password = "prova";
    connectValue.logStreamDebug = true;
    connect@MongoDB(connectValue)()

}


main {
scope (InsertMongoTest){
  install (default => valueToPrettyString@StringUtils(InsertMongoTest)(s);
          valueToPrettyString@StringUtils(__responseSplitRowContent)(s1);
          println@Console(s1)();
           println@Console(s )());

           requestReadFile.filename =  "mail.csv";
           readFile@File(requestReadFile)(responseReadFile);
           requestSplitFileContent =responseReadFile;
           requestSplitFileContent.regex = "\\r\\n";
           split@StringUtils(requestSplitFileContent)(__responseSplitFileContent);
           for (counter = 1 , counter < #__responseSplitFileContent.result, counter++  ){
             requestSplitRowContent =__responseSplitFileContent.result[counter];

             requestSplitRowContent.regex = ";";
             q.collection = "Contact";
             //q.writeConcern.w = 1;
             split@StringUtils(requestSplitRowContent)(__responseSplitRowContent);

             with (q.document){
               if (__responseSplitRowContent.result[1] != ""){
               trim@StringUtils(__responseSplitRowContent.result[1])(trimmedRegValue);
               toUpperCase@StringUtils(trimmedRegValue)(upperCaseRegValue);
               .regione = upperCaseRegValue

             };
               .status = __responseSplitRowContent.result[2];
               if (__responseSplitRowContent.result[3]!=""){
                  .intestazione = __responseSplitRowContent.result[3]
               };
               .cognome = __responseSplitRowContent.result[4];
               .nome = __responseSplitRowContent.result[5];
               if (__responseSplitRowContent.result[6]!=""){
                  .carica = __responseSplitRowContent.result[6]
               };
               if (__responseSplitRowContent.result[7]!=""){
                  .societa = __responseSplitRowContent.result[7]
               };
               if (__responseSplitRowContent.result[8]!=""){
                  .ind = __responseSplitRowContent.result[8]
               };
               if (__responseSplitRowContent.result[9]!=""){
                  .cap = __responseSplitRowContent.result[9]
               };
               if ((__responseSplitRowContent.result[10]!="")){
                 if (is_defined(__responseSplitRowContent.result[10])){
                    trim@StringUtils(__responseSplitRowContent.result[10])(trimmedCitValue);
                    toUpperCase@StringUtils(trimmedCitValue)(upperCaseCitValue);
                    .citta = upperCaseCitValue
                 }
               };

               if ((__responseSplitRowContent.result[11]!="")){
                  if (is_defined(__responseSplitRowContent.result[11])){
                      trim@StringUtils(__responseSplitRowContent.result[11])(trimmedProvValue);
                      toUpperCase@StringUtils(trimmedProvValue)(upperCaseProvValue);
                      .prov = upperCaseProvValue
                }
               };
               if (__responseSplitRowContent.result[12]!=""){
                  .tel = __responseSplitRowContent.result[12]
               };
               if (__responseSplitRowContent.result[14]!=""){
                  .mail = __responseSplitRowContent.result[14]
               }
             };
           insert@MongoDB(q)(responseq);
           undef(q)
      }


}
}
