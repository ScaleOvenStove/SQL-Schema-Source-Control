using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.Management.Smo;
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
        private static string _mainFilePath = ConfigurationManager.AppSettings["MainFolderPath"].ToString();
        private static bool _svnAuth = Convert.ToBoolean(ConfigurationManager.AppSettings["SVNAuthentication"].ToString());
        private static string _svnUserName = ConfigurationManager.AppSettings["SVNUserName"].ToString();
        private static string _svnPassword = ConfigurationManager.AppSettings["SVNPassword"].ToString();

        private static bool _emailOnError = Convert.ToBoolean(ConfigurationManager.AppSettings["EmailOnError"].ToString());
        private static string _emailServer = ConfigurationManager.AppSettings["EmailServer"].ToString();
        private static string _emailFrom = ConfigurationManager.AppSettings["EmailFrom"].ToString();
        private static string _emailTo = ConfigurationManager.AppSettings["EmailTo"].ToString();

        static void Main(string[] args)
        {
            try
            {
                XDocument runSteps = XDocument.Load(ConfigurationManager.AppSettings["RunStepsPath"].ToString());

                var LogJobServers = from c in runSteps.Elements("LogServers").Elements("LogJobs").Elements("server")
                                    select (string)c.Attribute("serverName");

                foreach (string serverName in LogJobServers)
                {
                    LogJobs(serverName);
                }

                var LogObjectServers = from c in runSteps.Elements("LogServers").Elements("LogObjects").Elements("server")
                                       select c;

                foreach (var logObjectServer in LogObjectServers)
                {
                    LogObjects((string)logObjectServer.Attribute("serverName"), (string)logObjectServer.Attribute("databaseName"));
                    //Console.WriteLine((string)logObjectServer.Attribute("serverName") + " " + (string)logObjectServer.Attribute("databaseName"));
                }

                // loop through all dirs in main folder and commit..
                string[] dirs = Directory.GetDirectories(_mainFilePath);

                foreach (string dir in dirs)
                {
                    ExecuteSVNCommit(dir);
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
                    string Subject = "SQLSchema2SVN Failed";
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

        private static void LogJobs(string serverName)
        {
            Console.WriteLine(DateTime.Now.ToString() + " Job Log Start for server " + serverName);

            Server srv = new Server(serverName);

            //Console.WriteLine(srv.JobServer);

            srv.SetDefaultInitFields(typeof(Job), "Name");

            JobServer agent = srv.JobServer;


            string dir = _mainFilePath + "\\" + serverName + "\\Jobs";

            DirectoryInfo d = new DirectoryInfo(dir);
            FileInfo[] files = d.GetFiles("*.sql");

            List<string> fileNames = new List<string>();
            List<string> jobFileNames = new List<string>();

            string svnCmd = string.Empty;

            foreach (FileInfo file in files)
            {
                fileNames.Add(file.Name);
            }

            foreach (Job j in agent.Jobs)
            {
                // TODO: check if date last modified is greater than last run
                // as queryable?
                jobFileNames.Add(j.Name.Replace(@"/", "_").Replace(@"\", "_") + ".sql");

                StringCollection sc = j.Script();

                foreach (string s in sc)
                {
                    //j.DateLastModified 
                    //j.JobID 
                    //j.VersionNumber 


                    // if file doesnt exist, svn add, svn commit
                    // if file exists and has changed,  svn commit
                    // check files against list of jobs, if not there, svn delete, svn commit

                    string filename = _mainFilePath + "\\" + serverName + @"\Jobs\" + j.Name.Replace(@"/", "_").Replace(@"\", "_") + ".sql";

                    string newFile = s;

                    if (!File.Exists(filename))
                    {
                        svnCmd = "add";
                        //ExecuteSVNCommand("add", filename);
                    }
                    else
                    {
                        StreamReader sr = new StreamReader(filename);

                        string existingFile = sr.ReadToEnd();
                        sr.Close();

                        if (existingFile != newFile)
                        {
                            svnCmd = "update";

                        }

                    }

                    //Console.WriteLine(svnCmd + " " + j.Name + " " + j.DateLastModified.ToString());

                    StreamWriter sw = new StreamWriter(filename, false);

                    sw.Write(s);
                    sw.Flush();
                    sw.Close();

                    ExecuteSVNCommand(svnCmd, filename);
                }

            }

            // handle the deletes for any files
            foreach (string file in fileNames)
            {
                //Console.WriteLine(jobFileNames.IndexOf(file).ToString());

                if (jobFileNames.IndexOf(file) == -1)
                {
                    svnCmd = "delete";
                    ExecuteSVNCommand(svnCmd, dir + @"\" + file);
                    Console.WriteLine(svnCmd + " " + file);
                }
            }

            Console.WriteLine(DateTime.Now.ToString() + " Job Log Done for server " + serverName);
        }

        private static void LogObjects(string serverName, string dbName)
        {
            Console.WriteLine(DateTime.Now.ToString() + " Object Log Start for server " + serverName + " database " + dbName);

            Server theServer = new Server(serverName);
            theServer.SetDefaultInitFields(typeof(StoredProcedure), "IsSystemObject");
            theServer.SetDefaultInitFields(typeof(UserDefinedFunction), "IsSystemObject");
            theServer.SetDefaultInitFields(typeof(View), "IsSystemObject");
            theServer.SetDefaultInitFields(typeof(Table), "IsSystemObject");

            Database myDB = theServer.Databases[dbName];

            // check if restoring, etc
            if (myDB.Status != DatabaseStatus.Normal)
            {
                return;
            }
            string dbPath = _mainFilePath + "\\" + serverName + @"\" + dbName;
            string svnCmd = string.Empty;

            if (!Directory.Exists(dbPath))
            {
                Directory.CreateDirectory(dbPath);
            }

            if (!Directory.Exists(dbPath + @"\StoredProcedures\"))
            {
                Directory.CreateDirectory(dbPath + @"\StoredProcedures\");
            }

            if (!Directory.Exists(dbPath + @"\Functions\"))
            {
                Directory.CreateDirectory(dbPath + @"\Functions\");
            }

            if (!Directory.Exists(dbPath + @"\Views\"))
            {
                Directory.CreateDirectory(dbPath + @"\Views\");
            }

            if (!Directory.Exists(dbPath + @"\Tables\"))
            {
                Directory.CreateDirectory(dbPath + @"\Tables\");
            }


            #region "Stored Procedures"

            DirectoryInfo d = new DirectoryInfo(dbPath + @"\StoredProcedures\");
            FileInfo[] files = d.GetFiles("*.sql");

            List<string> fileNames = new List<string>();
            List<string> objectFileNames = new List<string>();

            foreach (FileInfo file in files)
            {
                fileNames.Add(file.Name);
            }


            foreach (StoredProcedure sp in myDB.StoredProcedures)
            {

                if (sp.IsSystemObject == false)
                {
                    objectFileNames.Add(sp.Schema + "." + sp.Name.Replace(@"/", "_").Replace(@"\", "_") + ".sql");

                    //Console.WriteLine(sp.Name);


                    StringCollection sc = sp.Script();

                    foreach (string s in sc)
                    {
                        string newFile = s;
                        string filename = dbPath + @"\StoredProcedures\" + sp.Schema + "." + sp.Name.Replace(@"/", "_").Replace(@"\", "_") + ".sql";

                        if (!File.Exists(filename))
                        {
                            svnCmd = "add";
                            //ExecuteSVNCommand("add", filename);
                        }
                        else
                        {
                            StreamReader sr = new StreamReader(filename);

                            string existingFile = sr.ReadToEnd();
                            sr.Close();

                            if (existingFile != newFile)
                            {
                                svnCmd = "update";
                            }
                        }

                        //Console.WriteLine(s);

                        StreamWriter sw = new StreamWriter(filename, false);

                        sw.Write(s);
                        sw.Flush();
                        sw.Close();

                        ExecuteSVNCommand(svnCmd, filename);
                    }
                }
            }

            // handle the deletes for any files - stored procedures
            foreach (string file in fileNames)
            {
                //Console.WriteLine(jobFileNames.IndexOf(file).ToString());

                if (objectFileNames.IndexOf(file) == -1)
                {
                    svnCmd = "delete";
                    ExecuteSVNCommand(svnCmd, dbPath + @"\StoredProcedures\" + file);
                    Console.WriteLine(svnCmd + " " + file);
                }
            }
            #endregion

            #region "Functions"

            d = new DirectoryInfo(dbPath + @"\Functions\");
            files = d.GetFiles("*.sql");

            fileNames = new List<string>();
            objectFileNames = new List<string>();

            foreach (FileInfo file in files)
            {
                fileNames.Add(file.Name);
            }

            foreach (UserDefinedFunction f in myDB.UserDefinedFunctions)
            {
                if (f.IsSystemObject == false)
                {
                    objectFileNames.Add(f.Schema + "." + f.Name.Replace(@"/", "_").Replace(@"\", "_") + ".sql");

                    //Console.WriteLine(f.Name);
                    StringCollection sc = f.Script();

                    foreach (string s in sc)
                    {
                        string newFile = s;
                        string filename = dbPath + @"\Functions\" + f.Schema + "." + f.Name.Replace(@"/", "_").Replace(@"\", "_") + ".sql";

                        if (!File.Exists(filename))
                        {
                            svnCmd = "add";
                            //ExecuteSVNCommand("add", filename);
                        }
                        else
                        {
                            StreamReader sr = new StreamReader(filename);

                            string existingFile = sr.ReadToEnd();
                            sr.Close();

                            if (existingFile != newFile)
                            {
                                svnCmd = "update";
                            }
                        }

                        //Console.WriteLine(s);

                        StreamWriter sw = new StreamWriter(filename, false);

                        sw.Write(s);
                        sw.Flush();
                        sw.Close();

                        ExecuteSVNCommand(svnCmd, filename);
                    }
                }

            }

            // handle the deletes for any files - functions
            foreach (string file in fileNames)
            {
                //Console.WriteLine(jobFileNames.IndexOf(file).ToString());

                if (objectFileNames.IndexOf(file) == -1)
                {
                    svnCmd = "delete";
                    ExecuteSVNCommand(svnCmd, dbPath + @"\Functions\" + file);
                    Console.WriteLine(svnCmd + " " + file);
                }
            }

            #endregion

            #region "Views"

            d = new DirectoryInfo(dbPath + @"\Views\");
            files = d.GetFiles("*.sql");

            fileNames = new List<string>();
            objectFileNames = new List<string>();

            foreach (FileInfo file in files)
            {
                fileNames.Add(file.Name);
            }

            foreach (View v in myDB.Views)
            {
                if (v.IsSystemObject == false)
                {
                    objectFileNames.Add(v.Schema + "." + v.Name.Replace(@"/", "_").Replace(@"\", "_") + ".sql");

                    //Console.WriteLine(v.Name);
                    StringCollection sc = v.Script();

                    foreach (string s in sc)
                    {
                        string newFile = s;
                        string filename = dbPath + @"\Views\" + v.Schema + "." + v.Name.Replace(@"/", "_").Replace(@"\", "_") + ".sql";

                        if (!File.Exists(filename))
                        {
                            svnCmd = "add";
                            //ExecuteSVNCommand("add", filename);
                        }
                        else
                        {
                            StreamReader sr = new StreamReader(filename);

                            string existingFile = sr.ReadToEnd();
                            sr.Close();

                            if (existingFile != newFile)
                            {
                                svnCmd = "update";
                            }
                        }

                        //Console.WriteLine(s);

                        StreamWriter sw = new StreamWriter(filename, false);

                        sw.Write(s);
                        sw.Flush();
                        sw.Close();

                        ExecuteSVNCommand(svnCmd, filename);
                    }
                }

            }

            // handle the deletes for any files - views
            foreach (string file in fileNames)
            {
                //Console.WriteLine(jobFileNames.IndexOf(file).ToString());

                if (objectFileNames.IndexOf(file) == -1)
                {
                    svnCmd = "delete";
                    ExecuteSVNCommand(svnCmd, dbPath + @"\Views\" + file);
                    Console.WriteLine(svnCmd + " " + file);
                }
            }
            #endregion

            #region "Tables"

            d = new DirectoryInfo(dbPath + @"\Tables\");
            files = d.GetFiles("*.sql");

            fileNames = new List<string>();
            objectFileNames = new List<string>();

            foreach (FileInfo file in files)
            {
                fileNames.Add(file.Name);
            }

            foreach (Table t in myDB.Tables)
            {
                if (t.IsSystemObject == false)
                {
                    objectFileNames.Add(t.Schema + "." + t.Name.Replace(@"/", "_").Replace(@"\", "_") + ".sql");

                    //Console.WriteLine(t.Name);
                    StringCollection sc = t.Script();

                    foreach (string s in sc)
                    {
                        string newFile = s;
                        string filename = dbPath + @"\Tables\" + t.Schema + "." + t.Name.Replace(@"/", "_").Replace(@"\", "_") + ".sql";

                        if (!File.Exists(filename))
                        {
                            svnCmd = "add";
                            //ExecuteSVNCommand("add", filename);
                        }
                        else
                        {
                            StreamReader sr = new StreamReader(filename);

                            string existingFile = sr.ReadToEnd();
                            sr.Close();

                            if (existingFile != newFile)
                            {
                                svnCmd = "update";
                            }
                        }

                        //Console.WriteLine(s);

                        StreamWriter sw = new StreamWriter(filename, false);

                        sw.Write(s);
                        sw.Flush();
                        sw.Close();

                        ExecuteSVNCommand(svnCmd, filename);
                    }
                }
            }

            // handle the deletes for any files - tables
            foreach (string file in fileNames)
            {
                //Console.WriteLine(jobFileNames.IndexOf(file).ToString());

                if (objectFileNames.IndexOf(file) == -1)
                {
                    svnCmd = "delete";
                    ExecuteSVNCommand(svnCmd, dbPath + @"\Tables\" + file);
                    Console.WriteLine(svnCmd + " " + file);
                }
            }

            #endregion

            Console.WriteLine(DateTime.Now.ToString() + " Object Log Done for server " + serverName + " database " + dbName);
        }

        private static void ExecuteSVNCommand(string Cmd, string path)
        {

            ProcessStartInfo svnCmd = new ProcessStartInfo();
            svnCmd.FileName = "svn.exe";
            svnCmd.CreateNoWindow = true;
            svnCmd.WindowStyle = ProcessWindowStyle.Hidden;

            if (Cmd == "add")
            {
                // svn add
                if (_svnAuth)
                {
                    svnCmd.Arguments = "add \"" + path + "\" --username " + _svnUserName + " --password " + _svnPassword;
                }
                else
                {
                    svnCmd.Arguments = "add \"" + path + "\" ";
                }


                Process pCmd = Process.Start(svnCmd);

                //Wait for the process to end.
                pCmd.WaitForExit();

            }

            if (Cmd == "delete")
            {
                // svn delete

                if (_svnAuth)
                {
                    svnCmd.Arguments = "delete \"" + path + "\" --username " + _svnUserName + " --password " + _svnPassword;
                }
                else
                {
                    svnCmd.Arguments = "delete \"" + path + "\" ";
                }

                Process pCmd = Process.Start(svnCmd);

                //Wait for the process to end.
                pCmd.WaitForExit();
            }
        }

        private static void ExecuteSVNCommit(string dir)
        {
            Console.WriteLine(DateTime.Now.ToString() + " Commit Start for " + dir);

            ProcessStartInfo svnCommit = new ProcessStartInfo();
            svnCommit.FileName = "svn.exe";
            svnCommit.CreateNoWindow = true;
            svnCommit.WorkingDirectory = dir;
            svnCommit.WindowStyle = ProcessWindowStyle.Hidden;

            // svn commit
            if (_svnAuth)
            {
                svnCommit.Arguments = "commit \"" + dir + "\" --message \"\" --username " + _svnUserName + " --password " + _svnPassword;
            }
            else
            {
                svnCommit.Arguments = "commit \"" + dir + "\" --message \"\" ";
            }

            Process pCommit = Process.Start(svnCommit);

            //Wait for the process to end.
            pCommit.WaitForExit();

            Console.WriteLine(DateTime.Now.ToString() + " Commit Done for " + dir);
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
