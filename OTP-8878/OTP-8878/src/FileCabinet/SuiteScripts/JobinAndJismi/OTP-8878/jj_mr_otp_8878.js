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
 * @version 1.0   30-May-2025  :  The initial build was created by JJ0401
 * @version 1.1   12-June-2025 :  The code was refined and converted into
 *                                a modular structure
 * @version 1.2   27-June-2025 :  Formatted the email structure and structured the CSV file
 *
 *************************************************************************************/
define(["N/email", "N/file", "N/search"], /**
 * @param{email} email
 * @param{file} file
 * @param{search} search
 */ (email, file, search) => {
  /**
   * Defines the function that is executed at the beginning of the map/reduce process and
   * generates the input data.
   * @param {Object} inputContext
   * @param {boolean} inputContext.isRestarted - Indicates whether the current invocation
   * of this function is the first invocation (if true, the current invocation is not the
   * first invocation and this function has been restarted)
   * @param {Object} inputContext.ObjectRef - Object that references the input data
   * @typedef {Object} ObjectRef
   * @property {string|number} ObjectRef.id - Internal ID of the record instance that
   * contains the input data
   * @property {string} ObjectRef.type - Type of the record instance that contains the input
   * data
   * @returns {Array|Object|Search|ObjectRef|File|Query} The input data to use in the
   * map/reduce process
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
            ["mainline", "is", "T"],
            "AND",
            ["trandate", "within", "4/1/2025", "5/1/2025"],
          ],
          columns: [
            search.createColumn({ name: "entity", label: "Name" }),
            search.createColumn({ name: "email", label: "Email" }),
            search.createColumn({ name: "tranid", label: "Document Number" }),
            search.createColumn({ name: "amount", label: "Amount" }),
            search.createColumn({
              name: "salesrep",
              join: "customerMain",
              label: "Sales Rep",
            }),
            search.createColumn({ name: "internalid", label: "Internal ID" }),
          ],
        });

        let salesDetails = [];
        salesSearch.run().each(function (result) {
          let name = result.getText({ name: "entity" });
          let email = result.getValue({ name: "email" });
          let docNo = result.getValue({ name: "tranid" });
          let amount = result.getValue({ name: "amount" });
          let salesrep =
            result.getValue({ name: "salesrep", join: "customerMain" }) || -1;
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
   * Defines the function that is executed when the map entry point is triggered. This entry
   * point is triggered automatically when the associated getInputData stage is complete.
   * This function is applied to each key-value pair in the provided context.
   * @param {Object} mapContext - Data collection containing the key-value pairs to process
   * in the map stage. This parameter is provided automatically based on the results of the
   * getInputData stage.
   * @param {Iterator} mapContext.errors - Serialized errors that were thrown during previous
   * attempts to execute the map function on the current key-value pair
   * @param {number} mapContext.executionNo - Number of times the map function has been
   * executed on the current key-value pair.
   * @param {boolean} mapContext.isRestarted - Indicates whether the current invocation of
   * this function is the first invocation (if true, the current invocation is not the first
   * invocation and this function has been restarted)
   * @param {string} mapContext.key - Key to be processed during the map stage
   * @param {string} mapContext.value - Value to be processed during the map stage
   * @since 2015.2
   */

  const map = (mapContext) => {
    try {
      let prevSales = JSON.parse(mapContext.value);
      let salesRep = prevSales["salesrep"];
      mapContext.write({
        key: salesRep,
        value: JSON.stringify(prevSales),
      });
    } catch (e) {
      log.error("Error caught", e.message);
    }
  };

  /**
   * Defines the function that is executed when the reduce entry point is triggered. This
   * entry point is triggered automatically when the associated map stage is complete. This
   * function is applied to each group in the provided context.
   * @param {Object} reduceContext - Data collection containing the groups to process in
   * the reduce stage. This parameter is provided automatically based on the results of the map
   * stage.
   * @param {Iterator} reduceContext.errors - Serialized errors that were thrown during previous
   * attempts to execute the reduce function on the current group
   * @param {number} reduceContext.executionNo - Number of times the reduce function has been
   * executed on the current group
   * @param {boolean} reduceContext.isRestarted - Indicates whether the current invocation of
   * this function is the first invocation (if true, the current invocation is not the first
   * invocation and this function has been restarted)
   * @param {string} reduceContext.key - Key to be processed during the reduce stage
   * @param {List<String>} reduceContext.values - All values associated with a unique key that was
   * passed to the reduce stage
   *     for processing
   * @since 2015.2
   */
  const reduce = (reduceContext) => {
    try {
      let csvContent = `Customer Name,Customer Email,Document number of Transaction,Transaction Amount \n`;
      let csvName = "";
      let rep = "";
      let activeStatus;
      let fileAttachment;
      let repName;

      var emailAuth = -5;
      var emailRecipient = "";
      var emailSubject = "";
      var emailBody = "";

      reduceContext.values.forEach((value) => {
        let reducedParsedData = JSON.parse(value);

        csvContent += `${reducedParsedData.name},${reducedParsedData.email},${reducedParsedData.docNo},${reducedParsedData.amount} \n`;
        rep = reducedParsedData.salesrep;

        activeStatus = search.lookupFields({
          type: search.Type.EMPLOYEE,
          id: rep,
          columns: ["isinactive"],
        });

        inactiveCheck = activeStatus.isinactive;

        if (rep === -1) {
          csvName = `Previous Month Sales Details Unassigned.csv`;
        } else {
          if (inactiveCheck === true) {
            csvName = `Previous Month Sales Details Sales Rep Inactive.csv`;
          } else {
            csvName = `Previous Month Sales Details  ${reducedParsedData.salesrep}.csv`;
          }
        }

        customer = reducedParsedData.name;
        repName = getSalesRepName(rep);

        fileAttachment = createFile(csvContent, inactiveCheck, rep, repName);
      });

      /**
       * Function to create CSV files
       * @param {String} csvContents - content of the CSV file
       * @param {boolean} isInactive - indicates whether the sales rep is inactive
       * @param {int} repId - internal id of the sales rep
       * @param {String} repText - name of the sales rep
       * @returns {file Object}
       */
      function createFile(csvContents, isInactive, repId, repText) {
        try {
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

          var fileId = csvFile.save({
            ignoreMandatoryFields: true,
          });

          if (repId === -1) {
            emailRecipient = "andrew@test.com";
            emailSubject = "Assignment of sales rep";
            emailBody = `Dear Andrew, 
                         Hope this mail finds you in good health. This mail contains a CSV file
                         attached along with , containing the details of sales associated with the 
                         customers, who were not assigned with a sales rep.Please assign a sales 
                         rep to these customers.If you have any concerns , please send your queries 
                         to finman4725fa@oracle.com.
                         Best Regards,
                         Alex Wolfe`;
          } else {
            if (isInactive === true) {
              emailRecipient = "andrew@test.com";
              emailSubject = "Assignment of sales rep";
              emailBody = `Dear Andrew, 
                         Hope this mail finds you in good health. This mail contains a CSV file
                         attached along with , containing the details of sales associated with the 
                         customers who were assigned to ${repText}.However , ${repText} is currently 
                         inactive.So please give attention to assigning new sales reps to these 
                         customers.Please go through the CSV file and if you have any concerns , please 
                         feel free to reach out @finman4725fa@oracle.com.
                         Best Regards,
                         Alex Wolfe `;
            } else {
              emailRecipient = rep;
              emailSubject = "Sales by customer details from previous month";
              emailBody = `Dear ${repText}, 
                         Hope this mail finds you in good health. This mail contains a CSV file
                         attached along with , containing the details of sales associated with the 
                         customers reach out @finman4725fa@oracle.com.
                         Best Regards,
                         Alex Wolfe`;
            }
          }

          return csvFile;
        } catch (e) {
          log.error("Error caught", e.message);
        }
      }

      sendEmail(
        emailAuth,
        emailRecipient,
        emailSubject,
        emailBody,
        fileAttachment,
        repName
      );

      /**
       * Function to send the email
       * @param {int} auth - internal id of the sender
       * @param {int} recipient - internal id of the sales rep
       * @param {String} sub - subject of the email
       * @param {String} content - body of the message
       * @returns {file object} - the CSV file to be attached with the email
       */
      function sendEmail(auth, recipient, sub, content, attach) {
        try {
          email.send({
            author: auth,
            recipients: [recipient],
            subject: sub,
            body: content,
            attachments: [attach],
          });
        } catch (e) {
          log.error("Error caught", e.message);
        }
      }

      /**
       * Function to get the name of sales rep
       * @param {int} salesRepId - internal id of the sales rep
       * @returns {String}
       */
      function getSalesRepName(salesRepId) {
        try {
          let nameofRep = search.lookupFields({
            type: search.Type.EMPLOYEE,
            id: salesRepId,
            columns: ["entityid"],
          }).entityid;

          return nameofRep;
        } catch (e) {
          log.error("Error caught", e.message);
        }
      }
    } catch (e) {
      log.error("Error caught", e.message);
    }
  };

  /**
   * Defines the function that is executed when the summarize entry point is triggered.
   * This entry point is triggered automatically when the associated reduce stage is complete.
   * This function is applied to the entire result set.
   * @param {Object} summaryContext - Statistics about the execution of a map/reduce script
   * @param {number} summaryContext.concurrency - Maximum concurrency number when executing
   * parallel tasks for the map/reduce script
   * @param {Date} summaryContext.dateCreated - The date and time when the map/reduce script
   * began running
   * @param {boolean} summaryContext.isRestarted - Indicates whether the current invocation of
   * this function is the first invocation (if true, the current invocation is not the first
   * invocation and this function has been restarted)
   * @param {Iterator} summaryContext.output - Serialized keys and values that were saved as
   * output during the reduce stage
   * @param {number} summaryContext.seconds - Total seconds elapsed when running the map/reduce
   * script
   * @param {number} summaryContext.usage - Total number of governance usage units consumed
   * when running the map/reduce script
   * @param {number} summaryContext.yields - Total number of yields when running the map/reduce
   * script
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
