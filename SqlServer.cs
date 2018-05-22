using PetaPoco;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
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
        public override bool OnException(Exception x)
        {
            NLog.LogManager.GetCurrentClassLogger().Error(x);
            return base.OnException(x);
        }

        public bool WriteToServer(Dictionary<string,DataTable> dic)
        {
            try
            {
                OpenSharedConnection();
                foreach (var item in dic)
                {
                    using (var bulk = new SqlBulkCopy((SqlConnection)Connection))
                    {
                        WriteToServer(bulk, item.Key, item.Value);
                    }
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
            return true;
        }
        public bool WriteToServer(Dictionary<string, DataTable> dic, SqlBulkCopyOptions option)
        {
            try
            {
                BeginTransaction();
                foreach (var item in dic)
                {
                    using (var bulk = new SqlBulkCopy((SqlConnection)Connection, option, (SqlTransaction)Transaction))
                    {   
                        WriteToServer(bulk, item.Key, item.Value);
                    }
                }
                CompleteTransaction();
            }
            catch (Exception ex)
            {
                AbortTransaction();
                OnException(ex);
                throw;
            }
            return true;
        }
        public bool WriteToServer(string tableName,DataTable dt)
        {
            try
            {
                OpenSharedConnection();
                using (var bulk = new SqlBulkCopy((SqlConnection)Connection))
                {
                    WriteToServer(bulk, tableName, dt);
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
            return true;
        }
        public bool WriteToServer(string tableName, DataTable dt, SqlBulkCopyOptions option)
        {
            try
            {
                BeginTransaction();
                using (var bulk=new SqlBulkCopy((SqlConnection)Connection, option, (SqlTransaction)Transaction))
                {
                    WriteToServer(bulk, tableName, dt);
                }
                CompleteTransaction();
            }
            catch (Exception ex)
            {
                OnException(ex);
                AbortTransaction();
                throw;
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
