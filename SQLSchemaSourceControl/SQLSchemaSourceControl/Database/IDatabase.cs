using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Specialized;

namespace SQLSchemaSourceControl.Database
{
    interface IDatabase
    {
        NameValueCollection GetStoredProcedures();
        NameValueCollection GetFunctions();
        NameValueCollection GetViews();
        NameValueCollection GetTables();
        NameValueCollection GetPartitionSchemes();
        NameValueCollection GetJobs();
        NameValueCollection GetPartitionFunctions();
        NameValueCollection GetIndexes();
        NameValueCollection GetUsers();
        NameValueCollection GetDatabases();
        NameValueCollection GetLogins();
        NameValueCollection GetConfiguration();
        List<string> ListDatabasesForPattern(string pattern);

        bool SelectDatabase(string DatabaseName);
    }
}
