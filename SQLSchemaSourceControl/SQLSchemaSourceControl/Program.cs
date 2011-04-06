using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SMO = Microsoft.SqlServer.Management.Smo;
using System.Collections.Specialized;
using Microsoft.SqlServer.Management.Sdk.Sfc;
using Microsoft.SqlServer.Management.Smo.Agent;
using System.IO;
using System.Diagnostics;
using System.Configuration;
using System.Xml.Linq;
using System.Net.Mail;

namespace SQLSchemaSourceControl
{
    class Program
    {
        private static string _mainFilePath;
        private static bool _svnAuth;
        private static string _svnUserName;
        private static string _svnPassword;

        private static bool _emailOnError;
        private static string _emailServer;
        private static string _emailFrom;
        private static string _emailTo;

        static void Main(string[] args)
        {
            NameValueCollection appSettings = System.Configuration.ConfigurationManager.AppSettings;

            try
            {
                _mainFilePath = appSettings["MainFolderPath"];
                _svnAuth = bool.Parse(appSettings["SVNAuthentication"]);
                _svnUserName = appSettings["SVNUserName"];
                _svnPassword = appSettings["SVNPassword"];

                _emailOnError = bool.Parse(appSettings["EmailOnError"]);
                _emailServer = appSettings["EmailServer"];
                _emailFrom = appSettings["EmailFrom"];
                _emailTo = appSettings["EmailTo"];
            }
            catch (Exception e)
            {
                System.Console.WriteLine(string.Format("Config failure: \r\n\r\n{0}\r\n\r\n", e.ToString()));
                Environment.Exit(0);
            }

            System.Console.WriteLine("Config Ok");

            SourceControl.ISourceControl sourceControl = new SourceControl.SVN(_svnUserName, _svnPassword, _svnAuth);

            try
            {
                XDocument runSteps = XDocument.Load(ConfigurationManager.AppSettings["RunStepsPath"].ToString());

                var LogJobServers = from c in runSteps.Elements("LogServers").Elements("LogJobs").Elements("server")
                                    select (string)c.Attribute("serverName");


                foreach (string serverName in LogJobServers)
                {
                    string srvPath = _mainFilePath + "\\" + serverName;

                    Database.IDatabase db = new Database.SqlServer2008(serverName);

                    NameValueCollection nv;
                    nv = db.GetConfiguration();
                    if (nv != null)
                    {
                        sourceControl.LogObjects(srvPath, "Configuration", nv);
                    }
                    sourceControl.LogObjects(srvPath, "Databases", db.GetDatabases());
                    sourceControl.LogObjects(srvPath, "Logins", db.GetLogins());
                    sourceControl.LogObjects(srvPath, "Jobs", db.GetJobs());
                }

                var LogObjectServers = from c in runSteps.Elements("LogServers").Elements("LogObjects").Elements("server")
                                       select c;


                foreach (var logObjectServer in LogObjectServers)
                {
                    string serverName = (string)logObjectServer.Attribute("serverName");
                    string dbName = (string)logObjectServer.Attribute("databaseName");
                    string dbPath = _mainFilePath + "\\" + serverName + @"\" + dbName;
                    bool newDb = false;

                    if (!Directory.Exists(dbPath))
                    {
                        newDb = true;
                    }

                    Database.IDatabase db = new Database.SqlServer2008(serverName);
                    if (db.SelectDatabase(dbName))
                    {

                        Console.WriteLine(DateTime.Now.ToString() + " Object Log Start for server " + serverName + " database " + dbName);

                        try
                        {
                            sourceControl.LogObjects(dbPath, "StoredProcedures", db.GetStoredProcedures());
                            sourceControl.LogObjects(dbPath, "Functions", db.GetFunctions());
                            sourceControl.LogObjects(dbPath, "Views", db.GetViews());
                            sourceControl.LogObjects(dbPath, "Tables", db.GetTables());
                            sourceControl.LogObjects(dbPath, "PartitionFunctions", db.GetPartitionFunctions());
                            sourceControl.LogObjects(dbPath, "PartitionSchemes", db.GetPartitionSchemes());
                            sourceControl.LogObjects(dbPath, "Indexes", db.GetIndexes());
                            sourceControl.LogObjects(dbPath, "Users", db.GetUsers());
                            if (newDb)
                            {
                                sourceControl.Add(dbPath);
                            }
                        }
                        catch
                        {
                            // eh.  db is usually unavailable.
                        }
                    }
                }

                // loop through all dirs in main folder and commit..
                string[] dirs = Directory.GetDirectories(_mainFilePath);

                foreach (string dir in dirs)
                {
                    sourceControl.Commit(dir);
                }

                Console.WriteLine("done");
                //Console.ReadLine();

            }
            catch (Exception ex)
            {
                if (_emailOnError)
                {
                    string MailServer = _emailServer;
                    string From = _emailFrom;
                    string To = _emailTo;
                    string Subject = "SQLSchemaSourceControl Failed";
                    string Body = ex.Message + " " + ex.InnerException + " " + ex.StackTrace;
                    bool IsHtml = true;
                    SendMail(MailServer, From, To, Subject, Body, IsHtml);

                }
                else
                {
                    Console.WriteLine(ex.Message + " " + ex.InnerException + " " + ex.StackTrace);
                    throw ex;
                }


            }
        }

        public static void SendMail(string MailServer, string From, string To, string Subject, string Body, bool IsHtml)
        {
            MailMessage msg;
            using (msg = new MailMessage())
            {
                msg.To.Add(To);
                msg.From = new MailAddress(From);
                msg.Subject = Subject;
                msg.Body = Body;
                msg.IsBodyHtml = IsHtml;

                var smtpClient = new SmtpClient(MailServer);

                smtpClient.Send(msg);
            }
        }

    }
}
