/** 
 * @NApiVersion 2.1 
 * @NScriptType MapReduceScript 
 * @NModuleScope SameAccount 
 */ 
/************************************************************************************* 
 *  
 *  
 * ${OTP-8878} : ${Monthly Sales Notification for Sales Rep} 
 * 
 * 
**************************************************************************************
 * 
 * Author: Jobin and Jismi IT Services 
 * 
 * Date Created : 30-May-2025 
 * 
 * Description : This script is for sending monthly emails to sales reps with a CSV file 
 * attached, containing the details of sales associated with the customers assigned to 
 * the respective reps.If any customer does not have a sales rep assigned, the email will
 * be sent to a NetSuite admin, with a message to assign a sales rep to the customer. 
 * 
 * REVISION HISTORY 
 * 
 * @version 1.0   30-May-2025 :  The initial build was created by JJ0401 
 * @version 1.1   12-June-2025 : The code was refined and converted into
 *                               a modular structure 
 * 
 * 
 * 
 *************************************************************************************/ 
define(["N/email", "N/file", "N/record", "N/search"], 
 /**
 * @param{email} email
 * @param{file} file
 * @param{record} record
 * @param{search} search
 */ (email, file, record, search) => {
  /**
   * Defines the function that is executed at the beginning of the map/reduce process and generates the input data.
   * @param {Object} inputContext
   * @param {boolean} inputContext.isRestarted - Indicates whether the current invocation of this function is the first
   *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
   * @param {Object} inputContext.ObjectRef - Object that references the input data
   * @typedef {Object} ObjectRef
   * @property {string|number} ObjectRef.id - Internal ID of the record instance that contains the input data
   * @property {string} ObjectRef.type - Type of the record instance that contains the input data
   * @returns {Array|Object|Search|ObjectRef|File|Query} The input data to use in the map/reduce process
   * @since 2015.2
   */

  const getInputData = (inputContext) => {
    try {
      /**
       * Function to create a search
       * @param {}
       * @returns {array Object}
       */
      function fetchData() {
        let salesSearch = search.create({
          type: "transaction",
          filters: [
            ["type", "anyof", "SalesOrd"],
            "AND",
            ["trandate", "within", "lastmonth"],
            "AND",
            ["mainline", "is", "T"],
          ],
          columns: [
            search.createColumn({ name: "entity", label: "Name" }),
            search.createColumn({ name: "email", label: "Email" }),
            search.createColumn({ name: "tranid", label: "Document Number" }),
            search.createColumn({ name: "amount", label: "Amount" }),
            search.createColumn({ name: "salesrep", label: "Sales Rep" }),
            search.createColumn({ name: "internalid", label: "Internal ID" }),
          ],
        });

        let salesDetails = [];
        salesSearch.run().each(function (result) {
          let name = result.getText({ name: "entity" });
          let email = result.getValue({ name: "email" });
          let docNo = result.getValue({ name: "tranid" });
          let amount = result.getValue({ name: "amount" });
          let salesrep = result.getValue({ name: "salesrep" }) || "Unassigned";
          salesDetails.push({ name, email, docNo, amount, salesrep });

          return true;
        });

        return salesDetails;
      }

      let salesInfo = fetchData();

      return salesInfo;
    } catch (e) {
      log.error("Error caught", e.message);
    }
  };

  /**
   * Defines the function that is executed when the map entry point is triggered. This entry point is triggered automatically
   * when the associated getInputData stage is complete. This function is applied to each key-value pair in the provided
   * context.
   * @param {Object} mapContext - Data collection containing the key-value pairs to process in the map stage. This parameter
   *     is provided automatically based on the results of the getInputData stage.
   * @param {Iterator} mapContext.errors - Serialized errors that were thrown during previous attempts to execute the map
   *     function on the current key-value pair
   * @param {number} mapContext.executionNo - Number of times the map function has been executed on the current key-value
   *     pair
   * @param {boolean} mapContext.isRestarted - Indicates whether the current invocation of this function is the first
   *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
   * @param {string} mapContext.key - Key to be processed during the map stage
   * @param {string} mapContext.value - Value to be processed during the map stage
   * @since 2015.2
   */

  const map = (mapContext) => {
    try {
      let prevSales = JSON.parse(mapContext.value);
      let salesRep = prevSales["salesrep"];
      mapContext.write({
        key: salesRep || " ",
        value: JSON.stringify(prevSales),
      });
    } catch (e) {
      log.error("Error caught", e.message);
    }
  };

  /**
   * Defines the function that is executed when the reduce entry point is triggered. This entry point is triggered
   * automatically when the associated map stage is complete. This function is applied to each group in the provided context.
   * @param {Object} reduceContext - Data collection containing the groups to process in the reduce stage. This parameter is
   *     provided automatically based on the results of the map stage.
   * @param {Iterator} reduceContext.errors - Serialized errors that were thrown during previous attempts to execute the
   *     reduce function on the current group
   * @param {number} reduceContext.executionNo - Number of times the reduce function has been executed on the current group
   * @param {boolean} reduceContext.isRestarted - Indicates whether the current invocation of this function is the first
   *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
   * @param {string} reduceContext.key - Key to be processed during the reduce stage
   * @param {List<String>} reduceContext.values - All values associated with a unique key that was passed to the reduce stage
   *     for processing
   * @since 2015.2
   */
  const reduce = (reduceContext) => {
    try {
      var csvContent = "";
      var csvName = "";
      var rep = "";
      var customer = "";
      reduceContext.values.forEach((value) => {
        var parsedData = JSON.parse(value);

        csvContent += `Customer name: ${parsedData.name},Email: ${parsedData.email}, Document number: ${parsedData.docNo}, Amount: ${parsedData.amount} \n`;

        csvName = `Previous Month Sales Details ${parsedData.salesrep}.csv`;
        rep = parsedData.salesrep;
        customer = parsedData.name;
      });
      

      /**
       * Function to create CSV files
       * @param {}
       * @returns {file Object}
       */
      function createFile(csvContents) {
        try{
        var csvFile = file.create({
          name: csvName,
          fileType: file.Type.CSV,
          contents: csvContents,
          description:
            "This file contains the details of sales from previous month.",
          encoding: file.Encoding.UTF8,
          folder: -14,
          isOnline: true,

        });
        return csvFile
        }
        catch(e){
          log.error("Error caught",e.message)
        }
      }

      var newFile = createFile(csvContent);

      var fileId = newFile.save({
        ignoreMandatoryFields:true
      }); //saving the file

      var emailAuth = -5;
      var emailRecipient = "";
      var emailSubject = "";
      var emailBody = "";

      if (rep != "Unassigned") {

        emailRecipient = rep;
        emailSubject = "Sales by customer details from previous month";
        emailBody = "Details of customer sales from the previous month";
      } else {
        emailRecipient = "andrew@test.com";
        emailSubject = "Assignment of sales rep";
        emailBody = `Please assign a sales rep to ${customer}`;
      }

      /**
       * Function to send the email
       * @param {}
       * @returns {void}
       */
      function sendEmail(attach) {
        try{
        email.send({
          author: emailAuth,
          recipients: [emailRecipient],
          subject: emailSubject,
          body: emailBody,
          attachments: [attach],
        });
      }
      catch(e){
        log.error("Error caught",e.message);
      }
      }

      sendEmail(newFile);
    } catch (e) {
      log.error("Error caught", e.message);
    }
  };

  /**
   * Defines the function that is executed when the summarize entry point is triggered. This entry point is triggered
   * automatically when the associated reduce stage is complete. This function is applied to the entire result set.
   * @param {Object} summaryContext - Statistics about the execution of a map/reduce script
   * @param {number} summaryContext.concurrency - Maximum concurrency number when executing parallel tasks for the map/reduce
   *     script
   * @param {Date} summaryContext.dateCreated - The date and time when the map/reduce script began running
   * @param {boolean} summaryContext.isRestarted - Indicates whether the current invocation of this function is the first
   *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
   * @param {Iterator} summaryContext.output - Serialized keys and values that were saved as output during the reduce stage
   * @param {number} summaryContext.seconds - Total seconds elapsed when running the map/reduce script
   * @param {number} summaryContext.usage - Total number of governance usage units consumed when running the map/reduce
   *     script
   * @param {number} summaryContext.yields - Total number of yields when running the map/reduce script
   * @param {Object} summaryContext.inputSummary - Statistics about the input stage
   * @param {Object} summaryContext.mapSummary - Statistics about the map stage
   * @param {Object} summaryContext.reduceSummary - Statistics about the reduce stage
   * @since 2015.2
   */
  const summarize = (summaryContext) => {
    log.error("Mails sent successfully !");
  };

  return { getInputData, map, reduce, summarize };
});
