// See https://aka.ms/new-console-template for more information
using Newtonsoft.Json;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Net;
using System.Net.Http.Headers;



CDC_Demo CDC_Demo = new CDC_Demo();

CDC_Demo.GetDataFromAPI();
Environment.Exit(0);
public class CDC_Demo
{
    public static void GetDataFromAPI()
    {
        try
        {
            Console.WriteLine("Fetching data from API Start");
            string json = new WebClient().DownloadString("http://www.7timer.info/bin/api.pl?lon=113.17&lat=23.09&product=astro&output=json");

            Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(json);

            Console.WriteLine("Fetching data from API End");

            Console.WriteLine("Saving API data in Azure SQL Start");

            BulkDataInsertIntoSQL(myDeserializedClass);

            Console.WriteLine("Saving API data in Azure SQL End");

        }
        catch (Exception ex)
        {
            Console.WriteLine("Error :- " + ex.Message);
            Console.ReadLine();
        }
    }

    public static void BulkDataInsertIntoSQL(Root myDeserializedClass)
    {
        try
        {
            DataTable tbl = new DataTable();
            tbl.Columns.Add(new DataColumn("timepoint", typeof(Int32)));
            tbl.Columns.Add(new DataColumn("cloudcover", typeof(Int32)));
            tbl.Columns.Add(new DataColumn("seeing", typeof(Int32)));
            tbl.Columns.Add(new DataColumn("transparency", typeof(Int32)));
            tbl.Columns.Add(new DataColumn("lifted_index", typeof(Int32)));
            tbl.Columns.Add(new DataColumn("rh2m", typeof(Int32)));
            tbl.Columns.Add(new DataColumn("wind10mdirection", typeof(string)));
            tbl.Columns.Add(new DataColumn("wind10mspeed", typeof(Int32)));
            tbl.Columns.Add(new DataColumn("temp2m", typeof(Int32)));
            tbl.Columns.Add(new DataColumn("prec_type", typeof(string)));


            for (int i = 0; i < myDeserializedClass.dataseries.Count(); i++)
            {
                DataRow dr = tbl.NewRow();
                dr["timepoint"] = myDeserializedClass.dataseries[i].timepoint;
                dr["cloudcover"] = myDeserializedClass.dataseries[i].cloudcover;
                dr["seeing"] = myDeserializedClass.dataseries[i].seeing;
                dr["transparency"] = myDeserializedClass.dataseries[i].transparency;
                dr["lifted_index"] = myDeserializedClass.dataseries[i].lifted_index;
                dr["rh2m"] = myDeserializedClass.dataseries[i].rh2m;
                dr["wind10mdirection"] = myDeserializedClass.dataseries[i].wind10m.direction;
                dr["wind10mspeed"] = myDeserializedClass.dataseries[i].wind10m.speed;
                dr["temp2m"] = myDeserializedClass.dataseries[i].temp2m;
                dr["prec_type"] = myDeserializedClass.dataseries[i].prec_type;

                tbl.Rows.Add(dr);
            }

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = "40.124.98.11"; //"tcp:sqlserver-cbiedetest.database.windows.net";
            builder.UserID = "CGIAdmin";
            builder.Password = "CGITesting1@";
            builder.InitialCatalog = "CDCSourceDB";

            //string connection = "Data Source=40.124.138.4;Initial Catalog=StreamingTest;User Id = cgadmin; Password = CGInfinity1@";
            using (SqlConnection connection = new SqlConnection(builder.ConnectionString))
            {
                //create object of SqlBulkCopy which help to insert  
                SqlBulkCopy objbulk = new SqlBulkCopy(connection);

                //assign Destination table name  
                objbulk.DestinationTableName = "data";

                objbulk.ColumnMappings.Add("timepoint", "timepoint");
                objbulk.ColumnMappings.Add("cloudcover", "cloudcover");
                objbulk.ColumnMappings.Add("seeing", "seeing");
                objbulk.ColumnMappings.Add("transparency", "transparency");
                objbulk.ColumnMappings.Add("lifted_index", "lifted_index");
                objbulk.ColumnMappings.Add("rh2m", "rh2m");
                objbulk.ColumnMappings.Add("wind10mdirection", "wind10mdirection");
                objbulk.ColumnMappings.Add("wind10mspeed", "wind10mspeed");
                objbulk.ColumnMappings.Add("temp2m", "temp2m");
                objbulk.ColumnMappings.Add("prec_type", "prec_type");

                connection.Open();
                //insert bulk Records into DataBase.  
                objbulk.WriteToServer(tbl);
                connection.Close();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error :- " + ex.Message);
            Console.ReadLine();
        }
    }
}

// Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(myJsonResponse);
public class Dataseries
{
    public int timepoint { get; set; }
    public int cloudcover { get; set; }
    public int seeing { get; set; }
    public int transparency { get; set; }
    public int lifted_index { get; set; }
    public int rh2m { get; set; }
    public Wind10m wind10m { get; set; }
    public int temp2m { get; set; }
    public string prec_type { get; set; }
}

public class Root
{
    public string product { get; set; }
    public string init { get; set; }
    public List<Dataseries> dataseries { get; set; }
}

public class Wind10m
{
    public string direction { get; set; }
    public int speed { get; set; }
}

