using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SevereDiagnosisConsole
{
    class Program
    {


        static void Main(string[] args)
        {

            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = "your_server.database.windows.net";
            builder.UserID = "your_user";
            builder.Password = "your_password";
            builder.InitialCatalog = "your_database";


            string read = string.Empty;

            Console.WriteLine("Type Q or q to quit:");

            while(true)
            {
                Console.WriteLine("");
                Console.WriteLine("Enter member id:");

                read = Console.ReadLine();

                if (read.ToLower() == "q")
                {
                    break;
                }

                int memberId;
            
                if (int.TryParse(read, out memberId))
                {
                     GetMemberData(builder, memberId);
                }
                else
                {
                    Console.WriteLine("Enter a valid numerical value");
                }
            }

        }





        private static void GetMemberData(SqlConnectionStringBuilder builder, int memberId)
        {
            using (SqlConnection connection =
                new SqlConnection(
                    "")   //sensitive info
            )
            {

                string query =
                    @"Select d1.MemberID as 'Member ID', FirstName as 'First Name', LastName as 'Last Name',  d1.DiagnosisID as 'Most Severe Diagnosis ID', d1.DiagnosisDescription as 'Most Severe Diagnosis Description', d2.DiagnosisCategoryID as 'Category ID', d2.CategoryDescription as 'Category Description', d2.CategoryScore as 'Category Score', ISNull(d3.MostSevereCategory, 1) as 'Is Most Severe Category'

                From(Select m.[MemberID], m.[FirstName], m.[LastName], md.[DiagnosisID], d.[DiagnosisDescription] FROM[Member] m

                    LEFT JOIN[MemberDiagnosis] md  ON m.[MemberID] = md.[MemberID] AND md.[DiagnosisID] = (SELECT MIN([DiagnosisId]) FROM[MemberDiagnosis] WHERE[MemberID] = md.[MemberID])
                LEFT JOIN[Diagnosis] d ON d.[DiagnosisID] = md.[DiagnosisID]) d1

                    LEFT JOIN

                    (Select d.[DiagnosisID], dc.[DiagnosisCategoryID], dc.[CategoryDescription], dc.[CategoryScore]

                FROM[DiagnosisCategoryMap] dcm

                    LEFT Join[Diagnosis] d ON d.[DiagnosisID] = dcm.[DiagnosisID]

                JOIN[DiagnosisCategory] dc  ON dc.[DiagnosisCategoryID] = dcm.[DiagnosisCategoryID]) d2

                    ON d1.[DiagnosisID] = d2.[DiagnosisID]

                LEFT JOIN

                (SELECT
                    md.MemberId, MIN(dc.DiagnosisCategoryID) as MostSevereCategory FROM
                    MemberDiagnosis md
                    LEFT JOIN DiagnosisCategoryMap dcm ON dcm.DiagnosisID = md.DiagnosisID
                LEFT JOIN DiagnosisCategory dc ON dcm.DiagnosisCategoryID = dc.DiagnosisCategoryID
                GROUP BY md.MemberId) d3

                    ON d1.MemberID = d3.MemberID WHERE d1.MemberID = @memberID";


                try
                {

                    SqlCommand command = new SqlCommand(query, connection);
                    command.Parameters.AddWithValue("@memberId", memberId);

                    connection.Open();

                    Int32 rows = Convert.ToInt32(command.ExecuteScalar());


                    if (rows > 0)
                    {
                        // get data stream
                        var reader = command.ExecuteReader();

                        // write each record
                        while (reader.Read())
                        {
                            Console.WriteLine(
                                "Member ID: {0} | First Name: {1}|  Last Name: {2} | Most Severe Diagnosis ID: {3} |  Most Severe Diagnosis Description: {4} | Category ID : Category Description: {5} |  Is Most Severe Category: {6} ",
                                reader[0], reader[1], reader[2], reader[3], reader[4], reader[5], reader[6]);
                        }
                    }

                    else
                    {

                         Console.WriteLine("No records found that match that id: {0}", memberId);
                    }

               


                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }
        }

    }
}
