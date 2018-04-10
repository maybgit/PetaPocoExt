using PetaPoco;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mayb.DAL
{
    public abstract class SqlServer : Database
    {
        public SqlServer(string connectionStringName) : base(connectionStringName) { }
        public override void OnExecutingCommand(IDbCommand cmd)
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
        }
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
            NLog.LogManager.GetCurrentClassLogger().Error(x);
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
        public bool WriteToServer(Dictionary<string,DataTable> dic)
        {
            try
            {
                OpenSharedConnection();
                foreach (var item in dic)
                    WriteToServer(new SqlBulkCopy((SqlConnection)Connection), item.Key, item.Value);
            }
            catch (Exception ex)
            {
                OnException(ex);
                return false;
            }
            finally
            {
                CloseSharedConnection();
            }
            return true;
        }
        public bool WriteToServer(Dictionary<string, DataTable> dic, SqlBulkCopyOptions option)
        {
            try
            {
                BeginTransaction();
                foreach (var item in dic)
                    WriteToServer(new SqlBulkCopy((SqlConnection)Connection, option, (SqlTransaction)Transaction), item.Key, item.Value);
                CompleteTransaction();
            }
            catch (Exception ex)
            {
                OnException(ex);
                AbortTransaction();
                return false;
            }
            return true;
        }
        public bool WriteToServer(string tableName,DataTable dt)
        {
            try
            {
                OpenSharedConnection();
                WriteToServer(new SqlBulkCopy((SqlConnection)Connection), tableName, dt);
            }
            catch (Exception ex)
            {
                OnException(ex);
                return false;
            }
            finally
            {
                CloseSharedConnection();
            }
            return true;
        }
        public bool WriteToServer(string tableName, DataTable dt, SqlBulkCopyOptions option)
        {
            try
            {
                BeginTransaction();
                WriteToServer(new SqlBulkCopy((SqlConnection)Connection, option, (SqlTransaction)Transaction), tableName, dt);
                CompleteTransaction();
            }
            catch (Exception ex)
            {
                OnException(ex);
                AbortTransaction();
                return false;
            }
            return true;
        }
        public void WriteToServer(SqlBulkCopy bulk,string tableName, DataTable dt)
        {
            bulk.DestinationTableName = tableName;
            foreach (DataColumn item in dt.Columns) 
                bulk.ColumnMappings.Add(item.ColumnName, item.ColumnName);
            bulk.WriteToServer(dt);
        }
    }
}
