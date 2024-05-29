
//using MySql.Data.MySqlClient;
using MySqlConnector;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;

namespace SQLUpdate
{
    internal class Program
    {
        
        static void Main(string[] args)
        {
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            //QA TableName
            string tableName = "your Table";


            //QA Db string
            string connStr = "ConnectionString";
            MySqlConnection connection = new MySqlConnection(connStr);
            DataTable data = new DataTable();
            try
            {

                using (MySqlCommand command = new MySqlCommand("", connection))
                {
                    connection.Open();
                    data.Load(command.ExecuteReader());
                }
                connection.Close();           

                for (int i = 0; i < data.Rows.Count; i = i + 2000)
                {
                    IEnumerable<DataRow> items = data.AsEnumerable().Skip(i).Take(2000);                    
                    DataTable boundTable = items.CopyToDataTable<DataRow>();
                    UpdateBulkToMySQL(boundTable, tableName, "update Column Name", "ConditionColumn(PKId)", connStr);                  
                }
                
                
                stopWatch.Stop();
                TimeSpan ts = stopWatch.Elapsed;
                string elapsedTime = string.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                                     ts.Hours, ts.Minutes, ts.Seconds,
                                     ts.Milliseconds / 10);
                
            }
            catch (Exception ex)
            {
                // handle exception here
            }
        }

        /* -------------------------key note----------------------------
         * 
         * dt              = datatable with values to be updated.
         * tableName       = Table name to be updated in Database.
         * Value_Column    = column name to be updated.
         * conditionColumn = column name used to write 'WHERE' condition
         * ConnectionString= MySQL connection string
         * */
        public static bool UpdateBulkToMySQL(DataTable dt, string tableName, string valueColumn, string conditionColumn, string ConnectionString)
        {
            try
            {         

                int result = 0;

                if (dt.Rows.Count == 0 || tableName == string.Empty)
                    return false;

                //creating UPDATE Query Header accordingf to datatable column headers
                StringBuilder query = new StringBuilder("UPDATE " + tableName + " SET " + valueColumn + " = ( CASE " + conditionColumn + " ");

                List<string> cases = new List<string>();
                List<string> iDs = new List<string>();

                for (int n = 0; n < dt.Rows.Count; n++)
                {
                    string key = string.Empty;
                    string value = string.Empty;

                    key = dt.Rows[n][conditionColumn].ToString();
                    key = key.Insert(0, "'");
                    key += "'";
                    value = "your Value";

                    string CASE = "WHEN " + key + " THEN '" + value + "'";

                    cases.Add(CASE);

                    iDs.Add(key);
                }

                query.Append(String.Join(" ", cases));
                query.Append(" END ) WHERE " + conditionColumn + " IN ( ");

                query.Append(String.Join(",", iDs));
                query.Append(" );");

                //Updating

                MySqlConnection mConnection = new MySqlConnection(ConnectionString);
                mConnection.Open();
                using (MySqlCommand myCmd = new MySqlCommand(query.ToString(), mConnection))
                {
                    myCmd.CommandType = CommandType.Text;
                    result = myCmd.ExecuteNonQuery();
                }
                mConnection.Close();

                if (result > 0)
                    return true;
                else
                    return false;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}
