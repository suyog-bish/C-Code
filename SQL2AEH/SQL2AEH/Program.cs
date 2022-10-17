using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SQL2AEH
{
    public static class IEnumerableExtensions
    {
        public static IEnumerable<IEnumerable<T>> Split<T>(this IEnumerable<T> list, int partsSize)
        {
            return list.Select((item, index) => new { index, item })
                       .GroupBy(x => x.index / partsSize)
                       .Select(x => x.Select(y => y.item));
        }
    }

    public class SqlTextQuery
    {
        private readonly string _sqlDatabaseConnectionString;

        public SqlTextQuery(string sqlDatabaseConnectionString)
        {
            _sqlDatabaseConnectionString = sqlDatabaseConnectionString;
        }

        public IEnumerable<Dictionary<string, object>> PerformQuery(string query)
        {
            var command = new SqlCommand(query);
            //CommandType.Text;

            IEnumerable<Dictionary<string, object>> result = null;
            using (var sqlConnection = new SqlConnection(_sqlDatabaseConnectionString))
            {
                sqlConnection.Open();

                command.Connection = sqlConnection;
                using (SqlDataReader r = command.ExecuteReader())
                {
                    result = Serialize(r);
                }
                sqlConnection.Close();
            }
            return result;
        }

        private IEnumerable<Dictionary<string, object>> Serialize(SqlDataReader reader)
        {
            var results = new List<Dictionary<string, object>>();
            while (reader.Read())
            {
                var row = new Dictionary<string, object>();

                for (int i = 0; i < reader.FieldCount; i++)
                {
                    row.Add(reader.GetName(i), reader.GetValue(i));
                }
                results.Add(row);
            }
            return results;
        }
    }

    class Program
    {

        static void Main()
        {
            try
            {
                // CONFIGURABLE VARIABLES
                int SQLBatchSize = Convert.ToInt32(ConfigurationManager.AppSettings["SQLBatchSize"]);
                int ExecutionControl = Convert.ToInt32(ConfigurationManager.AppSettings["ExecutionControl"]);
                int ExecutionControlSleepMs = Convert.ToInt32(ConfigurationManager.AppSettings["ExecutionControlSleepMs"]);

                // VARIABLES
                string selectDataQuery;

                // GET SQL & HUB CONNECTION STRINGS
                SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
                builder.DataSource = "40.124.98.11"; //"tcp:sqlserver-cbiedetest.database.windows.net";
                builder.UserID = "CGIAdmin";
                builder.Password = "CGITesting1@";
                builder.InitialCatalog = "CDCSourceDB";
               
                //string sqlDatabaseConnectionString = ConfigurationManager.AppSettings["sqlDatabaseConnectionString"];
                Console.WriteLine(" SQL Connection String : - " + builder.ConnectionString);
                string sqlDatabaseConnectionString = builder.ConnectionString;
                string serviceBusConnectionString = ConfigurationManager.AppSettings["Microsoft.ServiceBus.ServiceBusConnectionString"];
                string hubName = ConfigurationManager.AppSettings["Microsoft.ServiceBus.EventHubToUse"];
                Console.WriteLine("Event hub connection string" + serviceBusConnectionString);
                Console.WriteLine("Event hub name" + hubName);
                // GET SQL SERVER SOURCE TABLE
                string dataTableName = ConfigurationManager.AppSettings["DataTableName"];
                string dataTableName_CT = "cdc." + dataTableName.Replace(".", "_") + "_CT";

                // ESTABLISH SQL & HUB CONNECTIONS
                SqlTextQuery queryPerformer = new SqlTextQuery(sqlDatabaseConnectionString);
                EventHubClient eventHubClient = EventHubClient.CreateFromConnectionString(serviceBusConnectionString, hubName);

                do
                {
                    Console.WriteLine("Data fetching from CDC SQL table start");

                    selectDataQuery = "GetCDCTableData";
                    IEnumerable<Dictionary<string, object>> resultCollection = queryPerformer.PerformQuery(selectDataQuery);

                    Console.WriteLine("Data fetching from CDC SQL table end");
                    
                    if (resultCollection.Any())
                    {                        
                        Console.WriteLine("Data sending to Event Hub in 5 batchsize");
                        foreach (var resultGroup in resultCollection.Split(SQLBatchSize))
                        {                            
                            SendRowsToEventHub(eventHubClient, resultGroup).Wait();                            
                        }
                        Console.WriteLine("Data sending to Event Hub completed");
                    }
                    
                    Thread.Sleep(ExecutionControlSleepMs);
                }
                while (ExecutionControl == 1); 
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error :- " + ex.Message);
            }
        }

        private static async Task SendRowsToEventHub(EventHubClient eventHubClient, IEnumerable<object> rows)
        {
            var memoryStream = new MemoryStream();

            using (var sw = new StreamWriter(memoryStream, new UTF8Encoding(false), 1024, leaveOpen: true))
            {
                string serialized = JsonConvert.SerializeObject(rows);
                sw.Write(serialized);
                sw.Flush();
            }

            Debug.Assert(memoryStream.Position > 0, "memoryStream.Position > 0");

            memoryStream.Position = 0;
            EventData eventData = new EventData(memoryStream);

            await eventHubClient.SendAsync(eventData);
        }
    }
}
