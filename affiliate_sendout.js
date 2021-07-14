function runQuery() {
  // Replace this value with the project ID listed in the Google
  // Cloud Platform project.
  a = { "xxx": "xxx@gmail.com"}


  // Iterate over the properties.
  for (var key in a) {
    console.log(a[key])
    



  var projectId = 'stitch-test-296708';
  var today = new Date().getDate(); 
  if (today == 1) {

  var request = {
    query: 'SELECT affiliatecode AS AffiliateCode, CAST(userid AS STRING) AS UserID, username AS Username, ROUND(SUM(deposit), 2) AS Deposits,  ROUND(SUM(bets), 2) AS Bets, ROUND(SUM(ggr), 2) AS GrossRev, ROUND(SUM(ggr-ngr), 2) AS BonusCosts, ROUND(SUM(ngr),2) AS NetRev, ROUND(SUM(ngr * primary_commission), 2) AS Commission, ROUND(SUM(ngr * secondary_commission), 2) AS SubAffCommission FROM stitch-test-296708.dbt_vip.affiliates WHERE primary_aff = "' + key + '" AND EXTRACT(MONTH from date) = EXTRACT (MONTH from CURRENT_DATE() - 1) GROUP BY 1, 2, 3 UNION ALL SELECT LEFT(primary_aff, 7) AS AffiliateCode, "Total" AS UserID, "-" AS Username, ROUND(SUM(deposit), 2) AS Deposits, ROUND(SUM(bets), 2) AS Bets, ROUND(SUM(ggr), 2) AS GrossRev, ROUND(SUM(ggr-ngr), 2) AS BonusCosts, ROUND(SUM(ngr), 2) AS NetRev, ROUND(SUM(ngr * primary_commission), 2) AS Commission, ROUND(SUM(ngr * secondary_commission), 2) AS SubAffCommission FROM stitch-test-296708.dbt_vip.affiliates  WHERE primary_aff = "' + key + '" AND EXTRACT(MONTH from date) = EXTRACT (MONTH from CURRENT_DATE() - 1) GROUP BY 1, 2, 3 ORDER BY userid ASC;', useLegacySql: false
  };
  var queryResults = BigQuery.Jobs.query(request, projectId);
  var jobId = queryResults.jobReference.jobId;

  var request2 = {
    query: 'SELECT affiliatecode AS AffiliateCode, CAST(userid AS STRING) AS UserID, username AS Username, date AS Date, ROUND(deposit, 2) AS Deposits, ROUND(bets, 2) AS Bets, ROUND(ggr, 2) AS GrossRev, ROUND(ggr - ngr, 2) AS BonusCosts, ROUND(ngr,2 ) AS NetRev, ROUND(ngr * primary_commission, 2) AS Commission, IFNULL(ROUND(ngr * secondary_commission, 2), 0) AS SubAffCommission FROM stitch-test-296708.dbt_vip.affiliates WHERE primary_aff = "' + key + '" AND EXTRACT(DAY from CURRENT_DATE() - 1) = EXTRACT(DAY from date) AND EXTRACT(MONTH from date) = EXTRACT (MONTH from CURRENT_DATE() - 1) UNION ALL SELECT LEFT(primary_aff, 7) AS AffiliateCode, "Total" AS UserID, "-" AS Username, date AS Date, ROUND(SUM(deposit), 2) AS Deposits, ROUND(SUM(bets), 2) AS Bets, ROUND(SUM(ggr), 2) AS GrossRev, ROUND(SUM(ggr - ngr), 2) AS BonusCosts, ROUND(SUM(ngr), 2) AS NetRev, ROUND(SUM(ngr * primary_commission), 2) AS Commission, IFNULL(ROUND(SUM(ngr * secondary_commission), 2), 0) AS SubAffCommission FROM stitch-test-296708.dbt_vip.affiliates WHERE primary_aff = "' + key + '" AND (EXTRACT(DAY from CURRENT_DATE() - 1) = EXTRACT(DAY from date)) AND EXTRACT(MONTH from date) = EXTRACT (MONTH from CURRENT_DATE() - 1) GROUP BY 1, 2, 3, 4  ORDER BY userid ASC;', useLegacySql: false
  };
  var queryResults2 = BigQuery.Jobs.query(request2, projectId);
  var jobId2 = queryResults2.jobReference.jobId;
 
  } else {

  var request = {
    query: 'SELECT affiliatecode AS AffiliateCode, CAST(userid AS STRING) AS UserID, username AS Username, ROUND(SUM(deposit), 2) AS Deposits,  ROUND(SUM(bets), 2) AS Bets, ROUND(SUM(ggr), 2) AS GrossRev, ROUND(SUM(ggr-ngr), 2) AS BonusCosts, ROUND(SUM(ngr),2) AS NetRev, ROUND(SUM(ngr * primary_commission), 2) AS Commission, ROUND(SUM(ngr * secondary_commission), 2) AS SubAffCommission FROM stitch-test-296708.dbt_vip.affiliates WHERE primary_aff = "' + key + '" AND EXTRACT(DAY from date) <> EXTRACT(DAY from CURRENT_DATE()) AND EXTRACT(MONTH from date) = EXTRACT (MONTH from CURRENT_DATE()) GROUP BY 1, 2, 3 UNION ALL SELECT LEFT(primary_aff, 7) AS AffiliateCode, "Total" AS UserID, "-" AS Username, ROUND(SUM(deposit), 2) AS Deposits, ROUND(SUM(bets), 2) AS Bets, ROUND(SUM(ggr), 2) AS GrossRev, ROUND(SUM(ggr-ngr), 2) AS BonusCosts, ROUND(SUM(ngr), 2) AS NetRev, ROUND(SUM(ngr * primary_commission), 2) AS Commission, ROUND(SUM(ngr * secondary_commission), 2) AS SubAffCommission FROM stitch-test-296708.dbt_vip.affiliates  WHERE primary_aff = "' + key + '" AND EXTRACT(MONTH from date) = EXTRACT (MONTH from CURRENT_DATE()) AND EXTRACT(DAY from date) <> EXTRACT(DAY from CURRENT_DATE()) GROUP BY 1, 2, 3 ORDER BY userid ASC;', useLegacySql: false
  };
  var queryResults = BigQuery.Jobs.query(request, projectId);
  var jobId = queryResults.jobReference.jobId;

  var request2 = {
    query: 'SELECT affiliatecode AS AffiliateCode, CAST(userid AS STRING) AS UserID, username AS Username, date AS Date, ROUND(deposit, 2) AS Deposits, ROUND(bets, 2) AS Bets, ROUND(ggr, 2) AS GrossRev, ROUND(ggr - ngr, 2) AS BonusCosts, ROUND(ngr,2 ) AS NetRev, ROUND(ngr * primary_commission, 2) AS Commission, IFNULL(ROUND(ngr * secondary_commission, 2), 0) AS SubAffCommission FROM stitch-test-296708.dbt_vip.affiliates WHERE primary_aff = "' + key + '" AND EXTRACT(DAY from CURRENT_DATE() - 1) = EXTRACT(DAY from date) AND EXTRACT(MONTH from date) = EXTRACT (MONTH from CURRENT_DATE()) UNION ALL SELECT LEFT(primary_aff, 7) AS AffiliateCode, "Total" AS UserID, "-" AS Username, date AS Date, ROUND(SUM(deposit), 2) AS Deposits, ROUND(SUM(bets), 2) AS Bets, ROUND(SUM(ggr), 2) AS GrossRev, ROUND(SUM(ggr - ngr), 2) AS BonusCosts, ROUND(SUM(ngr), 2) AS NetRev, ROUND(SUM(ngr * primary_commission), 2) AS Commission, IFNULL(ROUND(SUM(ngr * secondary_commission), 2), 0) AS SubAffCommission FROM stitch-test-296708.dbt_vip.affiliates WHERE primary_aff = "' + key + '" AND (EXTRACT(DAY from CURRENT_DATE() - 1) = EXTRACT(DAY from date)) AND EXTRACT(MONTH from date) = EXTRACT (MONTH from CURRENT_DATE()) GROUP BY 1, 2, 3, 4  ORDER BY userid ASC;', useLegacySql: false
  };
  var queryResults2 = BigQuery.Jobs.query(request2, projectId);
  var jobId2 = queryResults2.jobReference.jobId;
    
    }

  // Check on status of the Query Job.
  var sleepTimeMs = 500;
  while (!queryResults.jobComplete) {
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId);
  }
  
  
  while (!queryResults.jobComplete) {
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId2);
  }

  // Get all the rows of results.
  var rows = queryResults.rows;
  while (queryResults.pageToken) {
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId, {
      pageToken: queryResults.pageToken
    });
    rows = rows.concat(queryResults.rows);
  }


  var rows2 = queryResults2.rows;
  while (queryResults2.pageToken) {
    queryResults2 = BigQuery.Jobs.getQueryResults(projectId, jobId2, {
      pageToken: queryResults2.pageToken
    });
    rows2 = rows2.concat(queryResults2.rows2);
  }

  if (rows, rows2) {
    var spreadsheet = SpreadsheetApp.create('Results: ' + key);
    var sheet = spreadsheet.getActiveSheet();
    sheet.setName("Current month");
    var sheet2 = spreadsheet.insertSheet();
    sheet2.setName("Previous day");

    var sheetID = spreadsheet.getId();

    var drivedoc = DriveApp.getFileById(sheetID);
    drivedoc.setSharing(DriveApp.Access.ANYONE_WITH_LINK, DriveApp.Permission.EDIT);

    // Append the headers.
    var headers = queryResults.schema.fields.map(function(field) {
      return field.name;
    });
    sheet.appendRow(headers);

    var headers2 = queryResults2.schema.fields.map(function(field) {
      return field.name;
    });
    sheet2.appendRow(headers2);



    // Append the results.
    var data = new Array(rows.length);
    for (var i = 0; i < rows.length; i++) {
      var cols = rows[i].f;
      data[i] = new Array(cols.length);
      for (var j = 0; j < cols.length; j++) {
        data[i][j] = cols[j].v;
      }
    }
    sheet.getRange(2, 1, rows.length, headers.length).setValues(data);

    var data2 = new Array(rows2.length);
    for (var i2 = 0; i2 < rows2.length; i2++) {
      var cols2 = rows2[i2].f;
      data2[i2] = new Array(cols2.length);
      for (var j2 = 0; j2 < cols2.length; j2++) {
        data2[i2][j2] = cols2[j2].v;
      }
    }
    sheet2.getRange(2, 1, rows2.length, headers2.length).setValues(data2);

    var email = a[key];

    var sendEmail = MailApp.sendEmail(email, "ShangriLaVIP.com player statistics affiliate " + key, 
        "[Russian version below] \n\nDear affiliate, \n \nPlease, follow this link to access your stats: " + spreadsheet.getUrl() + "\n \nPlease do not reply to this email, it was automatically generated by our system. If you have any questions or comments, please contact your affiliate manager. You will receive the daily email update only if your players had gaming activity the previous day. \n\nBest regards, \nShangriLaVIP.com \n\n\nУважаемый партнер, \n\nДанные по вашему аккаунту: " + spreadsheet.getUrl() + " \n\nПожалуйста, не отвечайте на это письмо, так как оно сгенерировано автоматически. Если у Вас есть какие-либо вопросы, свяжитесь с Вашим персональным менеджером. Вы будете получать ежедневное оповещение по электронной почте только в том случае, если ваши игроки играли в предыдущий день. \n\nС уважением, \nShangriLaVIP.com");

    console.log(a[key])

    Logger.log('Results spreadsheet created: %s',
        spreadsheet.getUrl());
  } else {
    Logger.log('No rows returned.');
  }
}}
