using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using PetaPoco;

namespace Mayb.DAL
{
    public abstract class DbExtension : Database
    {
        public DbExtension(string connectionStringName) : base(connectionStringName) { }

        /*public override void OnExecutingCommand(IDbCommand cmd)
        {
#if DEBUG
            try
            {
                System.Text.StringBuilder strb = new System.Text.StringBuilder();
                foreach (SqlParameter item in cmd.Parameters)
                    strb.AppendLine(string.Format("DECLARE {0} NVARCHAR(MAX) = N'{1}'", item.ParameterName, item.Value));
                strb.AppendLine(cmd.CommandText);
                NLog.LogManager.GetCurrentClassLogger().Info(strb.ToString());
            }
            catch (Exception ex)
            {
                NLog.LogManager.GetCurrentClassLogger().Error(ex);
            }
#endif
        }*/

        public DataSet QueryDataSet(string sql, params object[] args)
        {
            try
            {
                OpenSharedConnection();
                using (var cmd = CreateCommand(Connection, sql, args))
                {
                    SqlDataAdapter sda = new SqlDataAdapter((SqlCommand)cmd);
                    DataSet ds = new DataSet();
                    sda.Fill(ds);
                    return ds;
                }
            }
            catch (Exception ex)
            {
                OnException(ex);
                throw;
            }
            finally
            {
                CloseSharedConnection();
            }
        }

        public override bool OnException(Exception x)
        {
            //NLog.LogManager.GetCurrentClassLogger().Error(x);
            return base.OnException(x);
        }

        public DataTable QueryDataTable(string sql, params object[] args)
        {
            try
            {
                OpenSharedConnection();
                using (var cmd = CreateCommand(Connection, sql, args))
                {
                    DataTable dt = new DataTable();
                    dt.Load(cmd.ExecuteReader());
                    return dt;
                }
            }
            catch (Exception ex)
            {
                OnException(ex);
                throw;
            }
            finally
            {
                CloseSharedConnection();
            }
        }

        public void Insert(DataTable dt, string tableName, SqlBulkCopyOptions option, bool enableTran = false)
        {
            OpenSharedConnection();
            SqlBulkCopy bulk = enableTran ? new SqlBulkCopy((SqlConnection)Connection, option, (SqlTransaction)Transaction) : new SqlBulkCopy(Connection.ConnectionString, option);
            Insert(bulk, dt, tableName);
        }
        public void Insert(DataTable dt, string tableName)
        {
            OpenSharedConnection();
            SqlBulkCopy bulk = new SqlBulkCopy((SqlConnection)Connection);
            Insert(bulk, dt, tableName);
        }

        private void Insert(SqlBulkCopy bulk, DataTable dt, string tableName)
        {
            try
            {
                bulk.DestinationTableName = tableName;
                foreach (DataColumn item in dt.Columns) bulk.ColumnMappings.Add(item.ColumnName, item.ColumnName);
                bulk.WriteToServer(dt);
            }
            catch (Exception ex)
            {
                OnException(ex);
                throw;
            }
            finally
            {
                CloseSharedConnection();
            }
        }
    }
}
