using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Specialized;
using SMO = Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Smo.Agent;
using System.Data.SqlClient;
using System.Text.RegularExpressions;

namespace SQLSchemaSourceControl.Database
{
    class SqlServer2008 : IDatabase
    {
        SMO.Server _srv;
        SMO.Database _db;

        public SqlServer2008(string ServerName)
        {
            _srv = new SMO.Server(ServerName);
        }

        public bool SelectDatabase(string DatabaseName) 
        {
            if (_srv.Databases.Contains(DatabaseName))
            {
                _db = _srv.Databases[DatabaseName];
                return true;
            }
            else
            {
                return false;
            }
        }

        public NameValueCollection GetIndexes()
        {
            NameValueCollection nv = new NameValueCollection();

            foreach (SMO.Table t in _db.Tables)
            {
                if (!t.IsSystemObject)
                {
                    foreach (SMO.Index i in t.Indexes)
                    {
                        StringCollection sc = i.Script();

                        string newFile = "";

                        foreach (string s in sc)
                        {
                            newFile += s + "\r\n";
                        }

                        nv.Add(t.Schema + "." + t.Name, newFile);
                    }
                }
            }

            return nv;
        }

        public List<string> ListDatabasesForPattern(string pattern)
        {
            List<string> databases = new List<string>();

            foreach (SMO.Database d in _srv.Databases)
            {
                if(Regex.IsMatch(d.Name, pattern))
                {
                    databases.Add(d.Name);
                }
            }
            return databases;
        }

        public NameValueCollection GetDatabases()
        {
            NameValueCollection nv = new NameValueCollection();


            foreach (SMO.Database d in _srv.Databases)
            {
                try
                {
                    StringCollection sc = d.Script();

                    string newFile = "";

                    foreach (string s in sc)
                    {
                        newFile += s + "\r\n";
                    }

                    newFile = System.Text.RegularExpressions.Regex.Replace(newFile, "SIZE = \\d+KB", "SIZE = 512000KB", System.Text.RegularExpressions.RegexOptions.Singleline);

                    nv.Add(d.Name, newFile);
                }
                catch (Exception e)
                {
                    // Ignore failures, usually restoring databases
                    System.Console.WriteLine("Failed to script out database: [" + d.Name + "]");
                }
            }

            return nv;
        }

        public NameValueCollection GetLogins()
        {
            NameValueCollection nv = new NameValueCollection();


            foreach (SMO.Login l in _srv.Logins)
            {
                if (!l.IsSystemObject)
                {
                    SMO.ScriptingOptions so = new SMO.ScriptingOptions();
                    so.LoginSid = true;

                    StringCollection sc = l.Script(so);

                    string newFile = "";

                    foreach (string s in sc)
                    {
                        newFile += s + "\r\n";
                    }
                    newFile = System.Text.RegularExpressions.Regex.Replace(newFile, "PASSWORD=N'.*?', SID", "PASSWORD=N'******', SID", System.Text.RegularExpressions.RegexOptions.Singleline);
                    nv.Add(l.Name, newFile);

                }
            }

            return nv;
        }

        public NameValueCollection GetConfiguration()
        {
            NameValueCollection nv = new NameValueCollection();
            try
            {
                using (SqlConnection conn = new SqlConnection(string.Format("Integrated Security=SSPI;Initial Catalog=master;Data Source={0}", _srv.Name)))
                {

                    string sql = "EXEC SP_CONFIGURE 'show advanced options' , 1;"
                               + "RECONFIGURE;"
                               + "EXEC SP_CONFIGURE;"
                               + "EXEC SP_CONFIGURE 'show advanced options' , 0;"
                               + "RECONFIGURE;";

                    SqlCommand cmd = new SqlCommand(sql, conn);
                    conn.Open();

                    using (SqlDataReader dr = cmd.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            nv.Add(dr["name"].ToString(), string.Format("EXEC SP_CONFIGURE '{0}', {1}", dr["name"], dr["config_value"].ToString()));
                        }

                        dr.Close();
                    }

                    conn.Close();
                }
            }
            catch
            {
                nv = null;
            }
            return nv;
        }

        public NameValueCollection GetJobs()
        {
            NameValueCollection nv = new NameValueCollection();

            JobServer agent = _srv.JobServer;

            foreach(Job j in agent.Jobs) 
            {

                StringCollection sc = j.Script();

                string newFile = "";

                foreach (string s in sc)
                {
                    newFile += s + "\r\n";
                }

                nv.Add(j.Name, newFile);
            }

            return nv;
        }

        public NameValueCollection GetUsers()
        {
            NameValueCollection nv = new NameValueCollection();

            foreach (SMO.User u in _db.Users)
            {

                    StringCollection sc = u.Script();

                    string newFile = "";

                    foreach (string s in sc)
                    {
                        newFile += s + "\r\n";
                    }

                    nv.Add(u.Name, newFile);
            }

            return nv;
        }

        public NameValueCollection GetTables()
        {
            NameValueCollection nv = new NameValueCollection();

            foreach (SMO.Table t in _db.Tables)
            {
                if (!t.IsSystemObject)
                {
                    SMO.ScriptingOptions so = new SMO.ScriptingOptions();

                    so.DriAll = true;

                    StringCollection sc = t.Script(so);

                    string newFile = "";

                    foreach (string s in sc)
                    {
                        newFile += s + "\r\n";
                    }

                    nv.Add(t.Schema + "." + t.Name, newFile);
                }
            }

            return nv;
        }

        public NameValueCollection GetViews()
        {
            NameValueCollection nv = new NameValueCollection();

            foreach (SMO.View v in _db.Views)
            {
                if (!v.IsSystemObject)
                {
                    StringCollection sc = v.Script();

                    string newFile = "";

                    foreach (string s in sc)
                    {
                        newFile += s + "\r\n";
                    }

                    nv.Add(v.Schema + "." + v.Name, newFile);
                }
            }

            return nv;
        }

        public NameValueCollection GetFunctions()
        {
            NameValueCollection nv = new NameValueCollection();

            foreach (SMO.UserDefinedFunction f in _db.UserDefinedFunctions)
            {
                if (!f.IsSystemObject)
                {
                    StringCollection sc = f.Script();

                    string newFile = "";

                    foreach (string s in sc)
                    {
                        newFile += s + "\r\n";
                    }

                    nv.Add(f.Schema + "." + f.Name, newFile);
                }
            }

            return nv;
        }
        
        public NameValueCollection GetStoredProcedures()
        {
            NameValueCollection nv = new NameValueCollection();

            foreach (SMO.StoredProcedure sp in _db.StoredProcedures)
            {
                if (!sp.IsSystemObject)
                {
                    StringCollection sc = sp.Script();

                    string newFile = "";

                    foreach (string s in sc)
                    {
                        newFile += s + "\r\n";
                    }

                    nv.Add(sp.Schema + "." + sp.Name, newFile);
                }
            }            
            
            return nv;
        }

        public NameValueCollection GetPartitionSchemes()
        {
            NameValueCollection nv = new NameValueCollection();

            foreach (SMO.PartitionScheme ps in _db.PartitionSchemes)
            {
                StringCollection sc = ps.Script();

                string newFile = "";

                foreach (string s in sc)
                {
                    newFile += s + "\r\n";
                }

                nv.Add(ps.Name, newFile);
            }

            return nv;
        }

        public NameValueCollection GetPartitionFunctions()
        {
            NameValueCollection nv = new NameValueCollection();

            foreach (SMO.PartitionFunction pf in _db.PartitionFunctions)
            {
                StringCollection sc = pf.Script();

                string newFile = "";

                foreach (string s in sc)
                {
                    newFile += s + "\r\n";
                }

                nv.Add(pf.Name, newFile);
            }

            return nv;
        }
    }
}
