using System;
using System.Collections.Generic;
using System.Text;
using System.Data.OracleClient;
using System.Configuration;
using System.Data;


namespace SendSMSForMC
{
    class classDataAccess
    {
        OracleConnection connection;

        public static string IDALERT = "MASTER_CARD_ALERT";
        public static string MYPHONE = "0906094098";
        public static char DEFAULT_SMS_STATUS = 'N';


        void classDataAcess()
        {
            connection = new OracleConnection();
        }

        public bool OpenConnection(string DBName)
        {
            string connectionString = System.Configuration.ConfigurationManager.AppSettings[DBName];
            try
            {
                connection = new OracleConnection(connectionString);
                connection.Open();
                //hoand rem 25082016
                //classDataAccessLogWriter.WriteLog("Open Connection Successful for " + connection.ConnectionString);
                return true;
            }
            catch(Exception ex)
            {
                classDataAccessLogWriter.WriteLog("Error OpenConnection(), "  + ex.Message);
                return false;
            }
        }

        public bool CloseConnection()
        {
            try
            {
                connection.Close();
                //hoand rem 25082016
                //classDataAccessLogWriter.WriteLog("Close Connection Successful for " + connection.ConnectionString);
                return true;
            }
            catch(Exception ex)
            {
                classDataAccessLogWriter.WriteLog("Error CloseConnection(), " + ex.Message);
                return false;
            }
        }
  
        public DataTable Find_OutBal_SMS(string pan, double amount, string apvCode, string transDate)
        {
            bool flag = false;
            DataTable data = new DataTable();
            try
            {
                flag=OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("Find_Existed_Out_Bal_Change", connection);

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                OracleParameter trans_amt_p = new OracleParameter("trans_amt_p", amount);
                trans_amt_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(trans_amt_p);

                OracleParameter approve_code_p = new OracleParameter("approve_code_p", apvCode);
                approve_code_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(approve_code_p);

                OracleParameter sms_date_p = new OracleParameter("sms_date_p", transDate);
                sms_date_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_date_p);

                cmd.CommandType = CommandType.StoredProcedure;
                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;

                cmd.Parameters.Add(results);
                OracleDataAdapter da = new OracleDataAdapter(cmd);
                
                da.Fill(data);

                if (flag == true)
                    CloseConnection();  
                return data;
            }
            catch (Exception ex)
            {
                classDWFPTLogWriter.WriteLog("Error Find_OutBal_SMS(), " + ex.Message);
                if (flag == true)
                    CloseConnection();  
                data.Clear();
                return data;
            }
            

        }
        
        public string GetCardNo(string en_card_no)
        {
            bool flag = false;
            try
            {
                string temp = null;
                flag=OpenConnection("CW_IM");// mo ket noi CSDL 
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");
                OracleCommand cmd = new OracleCommand("GetCardNo", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter updateTime = new OracleParameter("en_card_no_p", en_card_no);
                updateTime.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(updateTime);

                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter cardno = new OracleParameter("de_pan_p", OracleType.NVarChar, 16);
                cardno.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(cardno);
               
                cmd.ExecuteNonQuery();
                if (flag == true)
                    CloseConnection();
                temp = cmd.Parameters["de_pan_p"].Value.ToString();
                return temp;

            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error GetCardNo() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();               
                return null;

            }

        }
        public string Get_Not_New_Card(string pan)
        {
            bool flag = false;
            try
            {
                string temp = null;
                flag=OpenConnection("CW_IM");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");
                OracleCommand cmd = new OracleCommand("Get_Not_New_Card", connection);

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter count_p = new OracleParameter("count_p", OracleType.NVarChar, 17);
                count_p.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(count_p);
              
                cmd.ExecuteNonQuery();

                if (flag == true)
                    CloseConnection();
                temp = cmd.Parameters["count_p"].Value.ToString();
                return temp;

            }
            catch (Exception ex)
            {
                classKichHoatTheLogWriter.WriteLog("Error Get_Not_New_Card(), " + ex.Message);
                if (flag == true)
                    CloseConnection();  
                return null;
            }

        }
        public string Get_Max_UpdateTimeErr(string smsType)
        {
            bool flag = false;
            try
            {
                string temp = null;
                flag=OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                //OracleCommand cmd = new OracleCommand("Get_Max_UpdateTime", connection);
                OracleCommand cmd = new OracleCommand("Get_Max_UpdateTimeErr", connection);

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", smsType);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter update_time = new OracleParameter("update_time_p", OracleType.NVarChar, 17);
                update_time.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(update_time);
            
                cmd.ExecuteNonQuery();
                if (flag == true)
                    CloseConnection();
                temp = cmd.Parameters["update_time_p"].Value.ToString();
                return temp;

            }
            catch (Exception ex)
            {
                classDWFPTLogWriter.WriteLog("Error Get_Max_UpdateTime(), " + ex.Message);
                if (flag == true)
                    CloseConnection(); 
                return null;
            }

        }
       
        public string Check_Exist_Card(string cif)
        {
            bool flag = false;
            try
            {
                string temp = null;
                //OpenConnection("CW_IM_UAT");// mo ket noi CSDL
                flag=OpenConnection("CW_IM");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");
                OracleCommand cmd = new OracleCommand("Check_Cif_CW", connection);

                OracleParameter cif_p = new OracleParameter("cif_p", cif);
                cif_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(cif_p);

                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter count_p = new OracleParameter("count_p", OracleType.NVarChar, 17);
                count_p.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(count_p);
           
                cmd.ExecuteNonQuery();
                if (flag == true)
                    CloseConnection();
                temp = cmd.Parameters["count_p"].Value.ToString();
                return temp;

            }
            catch (Exception ex)
            {
                classKichHoatTheLogWriter.WriteLog("Error Check_Exist_Card(), " + ex.Message);
                if (flag == true)
                    CloseConnection();  
                return null;
            }

        } 
        public string Get_Reply_Card(string pan)
        {
            bool flag = false;
            try
            {
                string temp = null;
                flag=OpenConnection("CW_IM");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");
                OracleCommand cmd = new OracleCommand("Get_Reply_Card", connection);

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter count_p = new OracleParameter("count_p", OracleType.NVarChar, 17);
                count_p.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(count_p);
                

                cmd.ExecuteNonQuery();
                if (flag == true)
                    CloseConnection();
                temp = cmd.Parameters["count_p"].Value.ToString();
                return temp;

            }
            catch (Exception ex)
            {
                classKichHoatTheLogWriter.WriteLog("Error Get_Reply_Card(), " + ex.Message);
                if (flag == true)
                    CloseConnection(); 
                return null;
            }

        } 
        public string Get_First_Activate(string pan)
        {
            bool flag = false;
            try
            {
                string temp = null;
                flag=OpenConnection("CW_IM");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");
                OracleCommand cmd = new OracleCommand("Get_First_Activate", connection);

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter count_p = new OracleParameter("count_p", OracleType.NVarChar, 17);
                count_p.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(count_p);
                
                cmd.ExecuteNonQuery();
                if (flag == true)
                    CloseConnection();
                temp = cmd.Parameters["count_p"].Value.ToString();
                return temp;

            }
            catch (Exception ex)
            {
                classKichHoatTheLogWriter.WriteLog("Error Get_First_Activate(), " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return null;
            }

        }
        public string Get_Max_UpdateTime_Err(string smsType)
        {
            bool flag = false;
            try
            {
                string temp = null;
                flag=OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("Get_Max_UpdateTime_err", connection);

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", smsType);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter update_time = new OracleParameter("update_time_p", OracleType.NVarChar, 17);
                update_time.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(update_time);
                
                cmd.ExecuteNonQuery();
                if (flag == true)
                    CloseConnection();
                temp = cmd.Parameters["update_time_p"].Value.ToString();
                return temp;

            }
            catch (Exception ex)
            {
                classDWFPTLogWriter.WriteLog("Error Get_Max_UpdateTime(), " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return null;
            }

        }
        public string Get_Pass_Encode(string pass)
        {
            bool flag = false;
            try
            {
                string temp = null;
                flag=OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("Get_PASS_EC", connection);

                OracleParameter pass_p = new OracleParameter("pass_p", pass);
                pass_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pass_p);

                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter pass_encode_p = new OracleParameter("pass_encode_p", OracleType.NVarChar, 25);
                pass_encode_p.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(pass_encode_p);
                
                cmd.ExecuteNonQuery();
                if (flag == true)
                    CloseConnection();
                temp = cmd.Parameters["pass_encode_p"].Value.ToString();
                return temp;

            }
            catch (Exception ex)
            {
                classKichHoatTheLogWriter.WriteLog("Error Get_Pass_Encode(), " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return null;
            }

        }


        public string Get_Inf_Cus(string cif)
        {
            bool flag = false;
            try
            {
                string temp = null;
                //OpenConnection("CW_IM_UAT");// mo ket noi CSDL
                flag=OpenConnection("CW_IM");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");
                OracleCommand cmd = new OracleCommand("Get_Inf_Cus", connection);

                OracleParameter cif_P = new OracleParameter("cif_P", cif);
                cif_P.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(cif_P);

                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter phone_p = new OracleParameter("phone_p", OracleType.NVarChar, 17);
                phone_p.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(phone_p);
                
                cmd.ExecuteNonQuery();
                if (flag == true)
                    CloseConnection();
                temp = cmd.Parameters["phone_p"].Value.ToString();
                return temp;

            }
            catch (Exception ex)
            {
                classUpdatePhoneLogWriter.WriteLog("Error Get_Inf_Cus(), " + ex.Message);
                if (flag == true)
                    CloseConnection(); 
                return null;
            }

        }
        public string Get_Max_UpdateTime_Up(string UpdateType)
        {
            bool flag = false;
            try
            {
                string temp = null;
                flag=OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("Get_Max_UpdateTime_Up", connection);

                OracleParameter Update_TYPE_P = new OracleParameter("Update_TYPE_P", UpdateType);
                Update_TYPE_P.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(Update_TYPE_P);

                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter update_time = new OracleParameter("update_time_p", OracleType.NVarChar, 17);
                update_time.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(update_time);
                
                cmd.ExecuteNonQuery();
                if (flag == true)
                    CloseConnection();
                temp = cmd.Parameters["update_time_p"].Value.ToString();
                return temp;

            }
            catch (Exception ex)
            {
                classDWFPTLogWriter.WriteLog("Error Get_Max_UpdateTime_Up(), " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return null;
            }

        }
        public string Get_Max_UpdateTime_KichHoat(string smsType)
        {
            bool flag = false;
            try
            {
                string temp = null;
                flag=OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("Get_Max_UpdateTime_KichHoat", connection);

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", smsType);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter update_time = new OracleParameter("update_time_p", OracleType.NVarChar, 17);
                update_time.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(update_time);
                
                cmd.ExecuteNonQuery();
                if (flag == true)
                    CloseConnection();
                temp = cmd.Parameters["update_time_p"].Value.ToString();
                return temp;

            }
            catch (Exception ex)
            {
                classDWFPTLogWriter.WriteLog("Error Get_Max_UpdateTime_Pay(), " + ex.Message);
                if (flag == true)
                    CloseConnection();  
                return null;
            }

        }
        public string Get_Max_UpdateTime_Pay(string smsType)
        {
            bool flag = false;
            try
            {
                string temp = null;
                flag=OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("Get_Max_UpdateTime_Pay", connection);

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", smsType);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter update_time = new OracleParameter("update_time_p", OracleType.NVarChar, 17);
                update_time.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(update_time);
                
                cmd.ExecuteNonQuery();
                
                temp = cmd.Parameters["update_time_p"].Value.ToString();
                if (flag == true)
                    CloseConnection();
                return temp;

            }
            catch (Exception ex)
            {
                classDWFPTLogWriter.WriteLog("Error Get_Max_UpdateTime_Pay(), " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return null;
            }

        }
        public string Get_Max_UpdateTime_Out(string smsType)
        {
            bool flag = false;
            try
            {
                string temp = null;
                flag=OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("Get_Max_UpdateTime_Out", connection);

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", smsType);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter update_time = new OracleParameter("update_time_p", OracleType.NVarChar, 17);
                update_time.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(update_time);
               
                cmd.ExecuteNonQuery();            
                temp = cmd.Parameters["update_time_p"].Value.ToString();
                if (flag == true)
                    CloseConnection();
                return temp;

            }
            catch (Exception ex)
            {
                classDWFPTLogWriter.WriteLog("Error Get_Max_UpdateTime_Out(), " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return null;
            }

        }
        public string Get_Max_UpdateTime(string smsType)
        {
            bool flag = false;
            try
            {
                string temp = null;
                flag=OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("Get_Max_UpdateTime", connection);

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", smsType);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter update_time = new OracleParameter("update_time_p", OracleType.NVarChar, 17);
                update_time.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(update_time);
                
                cmd.ExecuteNonQuery();                
                temp = cmd.Parameters["update_time_p"].Value.ToString();
                if (flag == true)
                    CloseConnection();
                return temp;

            }
            catch (Exception ex)
            {
                classDWFPTLogWriter.WriteLog("Error Get_Max_UpdateTime(), " + ex.Message);
                if (flag == true)
                    CloseConnection(); 
                return null;
            }

        }
        public string Get_Max_UpdateTime_Dis(string smsType)
        {
            bool flag = false;
            try
            {
                string temp = null;
                flag=OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("Get_Max_UpdateTime_Dis", connection);

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", smsType);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter update_time = new OracleParameter("update_time_p", OracleType.NVarChar, 17);
                update_time.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(update_time);
               
                cmd.ExecuteNonQuery();
                if (flag == true)
                    CloseConnection();
                temp = cmd.Parameters["update_time_p"].Value.ToString();
                return temp;

            }
            catch (Exception ex)
            {
                classDWFPTLogWriter.WriteLog("Error Get_Max_UpdateTime_Dis(), at DataAccess" + ex.Message);
                if (flag == true)
                    CloseConnection();// dong ket noi CSDL  
                return null;
            }

        } 
      


        public string Get_Max_UpdateTime_Paymen(string smsType)
        {
            bool flag = false;
            try
            {
                string temp = null;
                flag=OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("Get_Max_UpdateTime", connection);

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", smsType);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter update_time = new OracleParameter("update_time_p", OracleType.NVarChar,17);
                update_time.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(update_time);
                
                cmd.ExecuteNonQuery();
                if (flag == true)
                    CloseConnection();
                temp = cmd.Parameters["update_time_p"].Value.ToString();
                return temp;
                
            }
            catch (Exception ex)
            {
                classOutstandingBalancePaymentManualLogWriter.WriteLog("Error Get_Max_UpdateTime_Paymen(), " + ex.Message);
                if (flag == true)
                    CloseConnection(); 
                return null;
            }

        }
        /*
        public DataTable Get_Max_OutBal_UpdateTime(string smsType)
        {
            try
            {
                OpenConnection("CW_DW");// mo ket noi CSDL
                OracleCommand cmd = new OracleCommand("Get_Max_Update_DateTime", connection);               

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", smsType);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                DataTable data = new DataTable();
                
                da.Fill(data);
                CloseConnection();// dong ket noi CSDL   
                return data;
            }
            catch (Exception ex)
            {
                classDataAccessLogWriter.WriteLog("Error Get_Max_OutBal_UpdateTime(), " + ex.Message);
                CloseConnection();// dong ket noi CSDL  
                return null;
            }
            
        }
         */
        //public DataTable Get_Inf_TXN_ISO()
        //{
        //    DataTable data = new DataTable();
        //    try
        //    {
        //        OpenConnection("CW_IM");// mo ket noi CSDL
        //        OracleCommand cmd = new OracleCommand("Get_Inf_TXN_ISO", connection);
        //        cmd.CommandType = CommandType.StoredProcedure;

        //        OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
        //        results.Direction = ParameterDirection.Output;
        //        cmd.Parameters.Add(results);

        //        OracleDataAdapter da = new OracleDataAdapter(cmd);

        //        da.Fill(data);
        //        CloseConnection();// dong ket noi CSDL   
        //        return data;
        //    }
        //    catch (Exception ex)
        //    {
        //        classDataAccessLogWriter.WriteLog("Error Get_Inf_TXN_ISO() at DataAccess, " + ex.Message);
        //        CloseConnection();
        //        data.Clear();
        //        return data;

        //    }

        //}
        //public DataTable Get_Inf_TXN_WEB()
        //{
        //    DataTable data = new DataTable();
        //    try
        //    {
        //        OpenConnection("CW_IM");// mo ket noi CSDL
        //        OracleCommand cmd = new OracleCommand("Get_Inf_TXN_WEB", connection);
        //        cmd.CommandType = CommandType.StoredProcedure;

        //        OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
        //        results.Direction = ParameterDirection.Output;
        //        cmd.Parameters.Add(results);

        //        OracleDataAdapter da = new OracleDataAdapter(cmd);

        //        da.Fill(data);
        //        CloseConnection();// dong ket noi CSDL   
        //        return data;
        //    }
        //    catch (Exception ex)
        //    {
        //        classDataAccessLogWriter.WriteLog("Error Get_Inf_TXN_WEB() at DataAccess, " + ex.Message);
        //        CloseConnection();
        //        data.Clear();
        //        return data;

        //    }

        //}
        //public DataTable GetOutstandingChange_Err(string updateDateTime)
        //{
        //    DataTable data = new DataTable();
        //    try
        //    {
        //        OpenConnection("CW_IM");// mo ket noi CSDL
        //        OracleCommand cmd = new OracleCommand("Get_Outstanding_Balance_Change", connection);
        //        OracleCommand cmd = new OracleCommand("Get_Outstanding_Bal_Change_Err", connection);
        //        cmd.CommandType = CommandType.StoredProcedure;

        //        OracleParameter updateTime = new OracleParameter("update_time", updateDateTime);
        //        updateTime.Direction = ParameterDirection.Input;
        //        cmd.Parameters.Add(updateTime);

        //        OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
        //        results.Direction = ParameterDirection.Output;
        //        cmd.Parameters.Add(results);

        //        OracleDataAdapter da = new OracleDataAdapter(cmd);
        //        da.Fill(data);
        //        CloseConnection();// dong ket noi CSDL   
        //        return data;
        //    }
        //    catch (Exception ex)
        //    {
        //        classOutstandingChangeLogWriter.WriteLog("Error GetOutstandingChange_Err() at DataAccess, " + ex.Message);
        //        CloseConnection();
        //        data.Clear();
        //        return data;

        //    }

        //}
        public DataTable GetOutstandingChange(string updateDateTime)
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {   
                ////OpenConnection("CW_DW");// mo ket noi CSDL 
                ////OracleCommand cmd = new OracleCommand("Get_Outstanding_Bal_Change_DW", connection);// thieu sms

                flag=OpenConnection("CW_IM");// mo ket noi CSDL               
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");            
                //OracleCommand cmd = new OracleCommand("Get_Outstanding_Bal_Change_MV", connection);//live
                OracleCommand cmd = new OracleCommand("Get_Outstanding_Bal_Chan_MV_T", connection); //test
                
                //OracleCommand cmd = new OracleCommand("Get_Outstanding_Bal_Ch_LOSS", connection);
                
                
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter updateTime = new OracleParameter("update_time", updateDateTime);
                updateTime.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(updateTime);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                
                da.Fill(data);
                if (flag == true)
                    CloseConnection();  
                return data;
            } 
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error GetOutstandingChange() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                data.Clear();
                return data;
                
            }
            
        }

        public DataTable get_sum_create_card(string p_create_date)
        {

            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_IM");
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");
                OracleCommand cmd = new OracleCommand("get_sum_create_card", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter create_date = new OracleParameter("p_date", p_create_date);
                create_date.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(create_date);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
               
                da.Fill(data);
                if (flag == true)
                    CloseConnection(); 
                return data;
            }
            catch (Exception ex)
            {
                classCheckDueDateLogWriter.WriteLog("Error get_sum_create_card() at DatAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                data.Clear();
                return data;
            }


        }

        public DataTable GetDueDate_DW()
        {

            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                flag=OpenConnection("CCPS_DW");
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CCPS_DW");
                OracleCommand cmd = new OracleCommand("GET_DUE_DATE", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                
                da.Fill(data);
                if (flag == true)
                    CloseConnection(); 
                return data;
            }
            catch (Exception ex)
            {
                classCheckDueDateLogWriter.WriteLog("Error GetDueDate_DW() at DatAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                data.Clear();
                return data;
            }
            
            
        }
        public DataTable GetNoQuaHan_IPP(string month, string updateDateTime)
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                flag = OpenConnection("CCPS_DW");
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CCPS_DW");
                OracleCommand cmd = new OracleCommand("GET_NO_QUA_HAN_IPP", connection);
                //OracleCommand cmd = new OracleCommand("GET_NO_QUA_HAN_IPP_T", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter updateTime = new OracleParameter("update_time", updateDateTime);
                updateTime.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(updateTime);

                OracleParameter p_month = new OracleParameter("p_month", month);
                p_month.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(p_month);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);

                da.Fill(data);
                if (flag == true)
                    CloseConnection();
                return data;
            }
            catch (Exception ex)
            {
                classNoQuaHanLogWriter.WriteLog("Error GetNoQuaHan_IPP() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                data.Clear();
                return data;
            }

        }
        public DataTable GetNoQuaHan(string updateDateTime)
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                flag=OpenConnection("CCPS_DW");
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CCPS_DW");
                OracleCommand cmd = new OracleCommand("GET_NOQUAHAN", connection);
                //OracleCommand cmd = new OracleCommand("GET_NOQUAHAN_T", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter updateTime = new OracleParameter("update_time", updateDateTime);
                updateTime.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(updateTime);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                
                da.Fill(data);
                if (flag == true)
                    CloseConnection();
                return data;
            }
            catch (Exception ex)
            {
                classNoQuaHanLogWriter.WriteLog("Error GetNoQuaHan() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                data.Clear();
                return data;
            }
           
        }
        public DataTable GetKichHoatThe(string updateDateTime)
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_IM");
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM"); 
                //OpenConnection("CW_IM_UAT");//hhhh
                OracleCommand cmd = new OracleCommand("GET_KICHHOATTHE", connection);
                //OracleCommand cmd = new OracleCommand("GET_KICHHOATTHE_T", connection);
                //OracleCommand cmd = new OracleCommand("GET_KICHHOATTHE_E", connection);
                
                
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter updateTime = new OracleParameter("update_time", updateDateTime);
                updateTime.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(updateTime);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                           
                da.Fill(data);
                if (flag == true)
                    CloseConnection();
                return data;
            }
            catch (Exception ex)
            {
                classKichHoatTheLogWriter.WriteLog("Error GetKichHoatThe()at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                data.Clear();
                return data;
            }
            
        }

        public DataTable GetAnnualFee(string updateDateTime)
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                OpenConnection("CW_IM");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");
                OracleCommand cmd = new OracleCommand("GET_TXN_ANNUAL_FEE", connection);
                //OracleCommand cmd = new OracleCommand("GET_TXN_ANNUAL_FEE_T", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter updateTime = new OracleParameter("update_time", updateDateTime);
                updateTime.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(updateTime);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
               
                da.Fill(data);
                if (flag == true)
                    CloseConnection(); 
                return data;
            }
            catch (Exception ex)
            {
                classAnnual_FeeLogWriter.WriteLog("Error GetAnnualFee()at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection(); 
                data.Clear();
                return data;
            }

        }
         public DataTable GetBlockAndFailAnnualFee(string updateDateTime)
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                ////OpenConnection("CW_DW");// mo ket noi CSDL                
                ////OracleCommand cmd = new OracleCommand("GET_Block_Fail_Fee_S", connection);

                flag=OpenConnection("CW_IM");// mo ket noi CSDL       
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");
                OracleCommand cmd = new OracleCommand("GET_Block_Fail_Fee", connection);
                //OracleCommand cmd = new OracleCommand("GET_Block_Fail_Fee_T", connection);

                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter updateTime = new OracleParameter("update_time", updateDateTime);
                updateTime.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(updateTime);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                
                da.Fill(data);
                if (flag == true)
                    CloseConnection(); 
                return data;
            }
            catch (Exception ex)
            {
                classBlockAndFailAnnualFeeLogWriter.WriteLog("Error GetBlockAndFailAnnualFee()at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                data.Clear();
                return data;
            }

        }

        public DataTable GetGDMCDebit(string updateDateTime)
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                //OpenConnection("CW_IM");// mo ket noi CSDL
                OpenConnection("CW_IM");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");
                OracleCommand cmd = new OracleCommand("Get_TXT_MC_DB", connection);
                //OracleCommand cmd = new OracleCommand("Get_TXT_MC_DB_New_T", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter updateTime = new OracleParameter("update_time", updateDateTime);
                updateTime.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(updateTime);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
              
                da.Fill(data);
                if (flag == true)
                    CloseConnection();   
                return data;
            }
            catch (Exception ex)
            {
                classGDMCDebitLogWriter.WriteLog("Error GetGDMCDebit()at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection(); 
                data.Clear();
                return data;
            }
        }
        public DataTable GetCapPhepGD(string updateDateTime)
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                //OpenConnection("CCPS_DW_UAT");// mo ket noi CSDL
                flag=OpenConnection("CCPS_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CCPS_DW");
                OracleCommand cmd = new OracleCommand("Get_CapPhepGD", connection);
                //OracleCommand cmd = new OracleCommand("Get_CapPhepGD_T", connection);//hhhh
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter updateTime = new OracleParameter("update_time", updateDateTime);
                updateTime.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(updateTime);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                
                da.Fill(data);

                if (flag == true)
                    CloseConnection(); 
                return data;
            }
            catch (Exception ex)
            {
                classCapPhepGDLogWriter.WriteLog("Error GetDKIPP()at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection(); 
                data.Clear();
                return data;
            }

        }
        public DataTable GetDKIPP(string updateDateTime)
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                flag=OpenConnection("CCPS_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CCPS_DW");
                OracleCommand cmd = new OracleCommand("Get_App_IPP", connection);
                //OracleCommand cmd = new OracleCommand("Get_App_IPP_T", connection);//hhhh
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter updateTime = new OracleParameter("update_time", updateDateTime);
                updateTime.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(updateTime);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);       
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                
                da.Fill(data);

                if (flag == true)
                    CloseConnection();   
                return data;
            }
            catch (Exception ex)
            {
                classDKIPPLogWriter.WriteLog("Error GetDKIPP()at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection(); 
                data.Clear();
                return data;
            }

        }
        public DataTable GetGDTangTien_New(string p_month,string p_Card_Brn)
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_IM");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");
                OracleCommand cmd = new OracleCommand("Get_GDTangTien_NEW", connection);
                //OracleCommand cmd = new OracleCommand("Get_GDTangTien_NEW_T", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter month = new OracleParameter("P_month", p_month);
                month.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(month);

                OracleParameter Card_Brn = new OracleParameter("Card_Brn", p_Card_Brn);
                Card_Brn.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(Card_Brn);
                
                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                
                da.Fill(data);
                if (flag == true)
                    CloseConnection();  
                return data;
            }
            catch (Exception ex)
            {
                classGDTangTienLogWriter.WriteLog("Error GetGDTangTien_New()at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();  
                data.Clear();
                return data;
            }

        }
        public DataTable GetGDTangTien(string updateDateTime)
        {
            DataTable data = new DataTable();
            try
            {
                OpenConnection("CW_IM");// mo ket noi CSDL
                OracleCommand cmd = new OracleCommand("Get_GDTangTien", connection);
                //OracleCommand cmd = new OracleCommand("Get_GDTangTien_T", connection);
                //OracleCommand cmd = new OracleCommand("Get_GDTangTien_NEW_T", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter updateTime = new OracleParameter("update_time", updateDateTime);
                updateTime.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(updateTime);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);

                da.Fill(data);
                CloseConnection();// dong ket noi CSDL   
                return data;
            }
            catch (Exception ex)
            {
                classGDTangTienLogWriter.WriteLog("Error GetGDTangTien()at DataAccess, " + ex.Message);
                CloseConnection();// dong ket noi CSDL  
                data.Clear();
                return data;
            }

        }
        
        public DataTable GetUpdatePhone(string updateDateTime)
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                flag=OpenConnection("CCPS_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CCPS_DW");
                //OpenConnection("CCPS_DW_UAT");//hhhh
                OracleCommand cmd = new OracleCommand("Get_UPDATE_PHONE", connection);
                //OracleCommand cmd = new OracleCommand("Get_UPDATE_PHONE_T", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter updateTime = new OracleParameter("update_time", updateDateTime);
                updateTime.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(updateTime);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
              
                da.Fill(data);
                if (flag == true)
                    CloseConnection();
                return data;
            }
            catch (Exception ex)
            {
                classUpdatePhoneLogWriter.WriteLog("Error GetUpdatePhone()at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection(); 
                data.Clear();
                return data;
            }

        }

        
        public DataTable GetGDHoanTra(string updateDateTime)
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_IM");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");  
                OracleCommand cmd = new OracleCommand("Get_GDHoanTra", connection);
                //OracleCommand cmd = new OracleCommand("Get_GDHoanTra_T", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter updateTime = new OracleParameter("update_time", updateDateTime);
                updateTime.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(updateTime);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);
                OracleDataAdapter da = new OracleDataAdapter(cmd);
                          
                da.Fill(data);
                if (flag == true)
                    CloseConnection(); 
                return data;
            }
            catch (Exception ex)
            {
                classGDHoanTraLogWriter.WriteLog("Error GetGDHoanTra() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                data.Clear();
                return data;
            }
            
        }
        public DataTable GetThuNoFail(string p_date, string p_update_time)
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_IM");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");
                OracleCommand cmd = new OracleCommand("get_Thu_No_Fail", connection);
                //OracleCommand cmd = new OracleCommand("get_Thu_No_Fail_T", connection);
                //OracleCommand cmd = new OracleCommand("get_Thu_No_Fail_E", connection);
                
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter date = new OracleParameter("p_date", p_date);
                date.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(date);

                OracleParameter update_time = new OracleParameter("p_update_time", p_update_time);
                update_time.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(update_time);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                
                da.Fill(data);
                if (flag == true)
                    CloseConnection();// dong ket noi CSDL   
                return data;
                
            }
            catch (Exception ex)
            {
                classThuNoFailLogWriter.WriteLog("Error GetThuNoFail()at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                data.Clear();
                return data;

            }

        }
        public DataTable GetExpiredExtension(string currentMonth)
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_IM");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM"); 
                OracleCommand cmd = new OracleCommand("get_Expired_Date", connection);
                //OracleCommand cmd = new OracleCommand("get_Expired_Date_T", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter expi_dt_p = new OracleParameter("expi_dt_p", currentMonth);
                expi_dt_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(expi_dt_p);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                          
                da.Fill(data);
                if (flag == true)
                    CloseConnection();  
                return data;
            }
            catch (Exception ex)
            {
                classExpiredExtensionLogWriter.WriteLog("Error GetExpiredExtension()at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                data.Clear();
                return data;

            }
            
        }
        public DataTable GetExpiredExtensionBoth()
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_IM");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");
                OracleCommand cmd = new OracleCommand("get_Expired_Date_Both", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                
                da.Fill(data);
                if (flag == true)
                    CloseConnection(); 
                return data;
            }
            catch (Exception ex)
            {
                classExpiredExtensionLogWriter.WriteLog("Error GetExpiredExtensionBoth() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                data.Clear();
                return data;

            }

        }
        public DataTable GetPaymentManualErr(string updateDateTime)
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_IM");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");
                //OracleCommand cmd = new OracleCommand("Get_Outstanding_Payment_Manual", connection);
                OracleCommand cmd = new OracleCommand("Get_Outstanding_Pay_Man_Err", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter updateTime = new OracleParameter("update_time", updateDateTime);
                updateTime.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(updateTime);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                
                da.Fill(data);
                if (flag == true)
                    CloseConnection();                
                return data;
            }
            catch (Exception ex)
            {
                classOutstandingBalancePaymentManualLogWriter.WriteLog("Error GetPaymentManualErr() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                data.Clear();
                return data;
            }

        }
        public DataTable GetThuNoTatToanIPP(string updateDateTime)
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                flag=OpenConnection("CCPS_DW");// mo ket noi CSDL    
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CCPS_DW");
                OracleCommand cmd = new OracleCommand("Get_ThuNo_TatToan_IPP", connection);//14/03/2016
                //OracleCommand cmd = new OracleCommand("Get_ThuNo_TatToan_IPP_T", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter updateTime = new OracleParameter("update_time", updateDateTime);
                updateTime.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(updateTime);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                
                da.Fill(data);
                if (flag == true)
                    CloseConnection();  
                return data;
            }
            catch (Exception ex)
            {
                classThuNoFailLogWriter.WriteLog("Error GetPaymentManual() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                data.Clear();
                return data;
            }

        }
        public DataTable GetPaymentManual(string updateDateTime)
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_IM");// mo ket noi CSDL   
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM"); 
                //OracleCommand cmd = new OracleCommand("Get_Payment_Manual_Online", connection);//14/03/2016
                OracleCommand cmd = new OracleCommand("Get_Payment_Manual_Online_T", connection);
                //OracleCommand cmd = new OracleCommand("Get_Payment_Manual_Online_E", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter updateTime = new OracleParameter("update_time", updateDateTime);
                updateTime.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(updateTime);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                              
                da.Fill(data);
                if (flag == true)
                    CloseConnection();  
                return data;
            }
            catch (Exception ex)
            {
                classOutstandingBalancePaymentManualLogWriter.WriteLog("Error GetPaymentManual() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                data.Clear();
                return data;
            }
           
        }
        public DataTable GetBinACQ()
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_AM");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_AM");
                OracleCommand cmd = new OracleCommand("Get_BIN_ACQ", connection);
                cmd.CommandType = CommandType.StoredProcedure;           

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
               
                da.Fill(data);
                if (flag == true)
                    CloseConnection(); 
                return data;
            }
            catch (Exception ex)
            {
                classOutstandingBalancePaymentAutoLogWriter.WriteLog("Error GetBinACQ() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                data.Clear();
                return data;
            }

        }
        public DataTable GetPaymentAuto(string updateDateTime)
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_IM");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");   
                OracleCommand cmd = new OracleCommand("Get_Outstanding_Payment_Auto", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter updateTime = new OracleParameter("update_time", updateDateTime);
                updateTime.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(updateTime);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                             
                da.Fill(data);
                if (flag == true)
                    CloseConnection();  
                return data;
            }
            catch (Exception ex)
            {
                classOutstandingBalancePaymentAutoLogWriter.WriteLog("Error GetPaymentAuto() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                data.Clear();
                return data;
            }
            
        }

        public DataTable GetReminderPayment1(string settleMonth, string p_crd_brn)
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                ////OpenConnection("CW_IM");// mo ket noi CSDL                
                ////OracleCommand cmd = new OracleCommand("Get_Reminder_Payment_1", connection);

               
                //OpenConnection("CW_IM");// mo ket noi CSDL
                flag = OpenConnection("CW_IM_STAN");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM_STAN");
                OracleCommand cmd = new OracleCommand("Get_Reminder_Payment_1_IPP", connection);
                //OracleCommand cmd = new OracleCommand("Get_Reminder_Payment_1_IPP_E", connection); //loai bo loc               
                //OracleCommand cmd = new OracleCommand("Get_Reminder_Payment_1_IPP_T", connection);               

                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter settl_month = new OracleParameter("settl_month", settleMonth);
                settl_month.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(settl_month);

                OracleParameter crd_brn = new OracleParameter("p_crd_brn", p_crd_brn);
                crd_brn.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(crd_brn);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                
                da.Fill(data);
                if (flag == true)
                    CloseConnection(); 
                return data;
            }
            catch (Exception ex)
            {
                classReminderPayment1LogWriter.WriteLog("Error GetReminderPayment1() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();  
                data.Clear();
                return data;
            }
           
        }

        public DataTable GetReminderPayment2(string settleMonth)
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_IM");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");  
                OracleCommand cmd = new OracleCommand("Get_Reminder_Payment_2", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter settl_month = new OracleParameter("settl_month", settleMonth);
                settl_month.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(settl_month);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                          
                da.Fill(data);
                if (flag == true)
                    CloseConnection(); 
                return data;
            }
            catch (Exception ex)
            {
                classReminderPayment2LogWriter.WriteLog("Error GetReminderPayment2() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                data.Clear();           
                return data;
            }
            
        }

        public DataTable GetReminderPayment2_IPP(string settleMonth, string p_due_dt)
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                //OpenConnection("CW_IM");// mo ket noi CSDL
                flag=OpenConnection("CW_IM_STAN");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM_STAN");
                OracleCommand cmd = new OracleCommand("Get_Reminder_Payment_2_IPP", connection);
                //OracleCommand cmd = new OracleCommand("Get_Reminder_Payment_2_IPP_Err", connection);                
                //OracleCommand cmd = new OracleCommand("Get_Reminder_Payment_2_IPP_T", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter settl_month = new OracleParameter("settl_month", settleMonth);
                settl_month.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(settl_month);

                OracleParameter due_dt = new OracleParameter("due_dt", p_due_dt);
                due_dt.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(due_dt);

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                
                da.Fill(data);
                if (flag == true)
                    CloseConnection();
                return data;
            }
            catch (Exception ex)
            {
                classReminderPayment2LogWriter.WriteLog("Error GetReminderPayment2() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection(); 
                data.Clear();
                return data;
            }

        }


        public DataTable GetDueDate()
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_IM");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM"); 
                OracleCommand cmd = new OracleCommand("Get_Due_Date", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                        
                da.Fill(data);
                if (flag == true)
                    CloseConnection();   
                return data;
            }
            catch (Exception ex)
            {
                classCheckDueDateLogWriter.WriteLog("Error GetReminderPayment2() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                data.Clear();
                return data;
            }
            
        }
        
        public DataTable GetRewardPoint()
        {
            DataTable data = new DataTable();
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_IM");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");
                OracleCommand cmd = new OracleCommand("Get_Reward_Point", connection);
                cmd.CommandType = CommandType.StoredProcedure;
                OracleParameter results = new OracleParameter("sys_cursor", OracleType.Cursor);
                results.Direction = ParameterDirection.Output;
                cmd.Parameters.Add(results);
                OracleDataAdapter da = new OracleDataAdapter(cmd);
                
                da.Fill(data);
                if (flag == true)
                    CloseConnection();  
                return data;
            }
            catch (Exception ex)
            {
                classAccumulativeRewardPointLogWriter.WriteLog("Error GetRewardPoint() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection(); 
                data.Clear();
                return data;
            }
            
        }


        public int InsertGDHoanTraSMSToDW(
                                          string sms_type,
                                          string sms_detail,
                                          string dest_mobile,//long dest_mobile,
                                          DateTime get_trans_datetime,
                                          string pan,
                                          string card_brn,
                                          string card_type,
                                          string sms_date,//long sms_date,
                                          string sms_time,//long sms_time,
                                          double trans_amt,//long trans_amt,
                                          string crncy_cde,//int crncy_cde,
                                          string merc_name,
                                          string approve_code,
                                          string trans_type,
                                          string trans_status,
                                          string update_datetime,
                                          string sms_stat
                                          )
        {
            bool flag = false;
            try
            {
                flag = OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                //OracleCommand cmd = new OracleCommand("fpt.Insert_GDHoanTra", connection);
                OracleCommand cmd = new OracleCommand("fpt.Insert_GDHoanTra2", connection);
                cmd.CommandType = CommandType.StoredProcedure;
                
                OracleParameter sms_type_p = new OracleParameter("sms_type_p", sms_type);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);
                
                OracleParameter sms_detail_p = new OracleParameter("sms_detail_p", sms_detail);
                sms_detail_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_detail_p);

                OracleParameter dest_mobile_p = new OracleParameter("dest_mobile_p", dest_mobile);
                dest_mobile_p.Direction = ParameterDirection.Input;
                //dest_mobile_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(dest_mobile_p);
                
                OracleParameter get_trans_datetime_p = new OracleParameter("get_trans_datetime_p", get_trans_datetime);
                get_trans_datetime_p.Direction = ParameterDirection.Input;
                //get_trans_datetime_p.OracleType = OracleType.DateTime;
                cmd.Parameters.Add(get_trans_datetime_p);

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                OracleParameter card_brn_p = new OracleParameter("card_brn_p", card_brn);
                card_brn_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_brn_p);

                OracleParameter card_type_p = new OracleParameter("card_type_p", card_type);
                card_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_type_p);

                OracleParameter sms_date_p = new OracleParameter("sms_date_p", sms_date);
                sms_date_p.Direction = ParameterDirection.Input;                
                cmd.Parameters.Add(sms_date_p);

                OracleParameter sms_time_p = new OracleParameter("sms_time_p", sms_time);
                sms_time_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_time_p);

                OracleParameter trans_amt_p = new OracleParameter("trans_amt_p", trans_amt);
                trans_amt_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(trans_amt_p);

                OracleParameter crncy_cde_p = new OracleParameter("crncy_cde_p", crncy_cde);
                crncy_cde_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(crncy_cde_p);

                OracleParameter merc_name_p = new OracleParameter("merc_name_p", merc_name);
                merc_name_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(merc_name_p);

                OracleParameter approve_code_p = new OracleParameter("approve_code_p", approve_code);
                approve_code_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(approve_code_p);

                OracleParameter trans_type_p = new OracleParameter("trans_type_p", trans_type);
                trans_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(trans_type_p);
                
                OracleParameter trans_status_p = new OracleParameter("trans_status_p", trans_status);
                trans_status_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(trans_status_p);
               
                
                OracleParameter update_datetime_p = new OracleParameter("update_datetime_p", update_datetime);
                update_datetime_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(update_datetime_p);

                OracleParameter sms_stat_p = new OracleParameter("sms_stat_p", sms_stat);
                sms_stat_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_stat_p);
                
                OracleString rowID;
                int insertedRow = 0;
               
           
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
            }
            catch (Exception ex)
            {
                classGDHoanTraLogWriter.WriteLog("Error InsertGDHoanTraSMSToDW() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }
            //CloseConnection();// dong ket noi CSDL   
           
        }
        public int Update_Status_SMS_All(
        string pan,
        string sms_update_time,//long sms_date,
        string sms_type,
        string sms_stat
        )
        {
            bool flag = false;
            try
            {
                flag = OpenConnection("CW_DW");
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("fpt.Update_Status_SMS_All", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                OracleParameter sms_date_p = new OracleParameter("sms_update_p", sms_update_time);
                sms_date_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_date_p);

                OracleParameter approve_code_p = new OracleParameter("sms_type_p", sms_type);
                approve_code_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(approve_code_p);

                OracleParameter sms_stat_p = new OracleParameter("sms_stat_p", sms_stat);
                sms_stat_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_stat_p);

                OracleString rowID;
                int insertedRow = 0;
                
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error Update_Status_SMS_All() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }
            //CloseConnection();// dong ket noi CSDL   
        }

       

        public int Update_CIF(string cif, string phone, string mail, string update_time)
        {
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_IM");// 
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");
                //OpenConnection("CW_IM_UAT"); //hhhh
                //OracleCommand cmd = new OracleCommand("Update_Inf_Cus", connection);
                OracleCommand cmd = new OracleCommand("Update_Inf_Cus_From_Fcc", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter cif_p = new OracleParameter("cif_p", cif);
                cif_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(cif_p);

                OracleParameter phone_p = new OracleParameter("phone_p", phone);
                phone_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(phone_p);

                OracleParameter upd_time_p = new OracleParameter("update_time_P", update_time);
                upd_time_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(upd_time_p);


                //OracleParameter mail_p = new OracleParameter("mail_p", mail);
                //mail_p.Direction = ParameterDirection.Input;
                //cmd.Parameters.Add(mail_p);          

                OracleString rowID;
                int insertedRow = 0;
                
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection(); 
                return insertedRow;
                
            }
            catch (Exception ex)
            {
                classUpdatePhoneLogWriter.WriteLog("Error Update_CIF() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();  
                return 0;
            }
            
        }
        public int Update_DISABLE_SpecialLimit(string pan, string create_date, string user_lv1)
        {
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_IM");// 
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");
                //OpenConnection("CW_IM_UAT");//hhhh
                OracleCommand cmd = new OracleCommand("Update_DISABLE_SpecialLimit", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                OracleParameter date_time_p = new OracleParameter("date_time_p", create_date);
                date_time_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(date_time_p);

                OracleParameter user_id_p = new OracleParameter("user_id_p", user_lv1);
                user_id_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(user_id_p);


                OracleString rowID;
                int insertedRow = 0;
                
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
               
            }
            catch (Exception ex)
            {
                classUpdatePhoneLogWriter.WriteLog("Error Update_DISABLE_SpecialLimit() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();  
                return 0;
            }

        }
        public int Update_DISABLE_CardReplacement(string pan, string user, string create_time)
        {
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_IM");// 
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");
                OracleCommand cmd = new OracleCommand("Update_DISABLE_CardReplacement", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter panP = new OracleParameter("pan_p", pan);
                panP.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(panP);

                OracleParameter user_p = new OracleParameter("user_p", user);
                user_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(user_p);

                OracleParameter cre_time_p = new OracleParameter("cre_time_p", create_time);
                cre_time_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(cre_time_p);

                OracleString rowID;
                int insertedRow = 0;
                
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                return insertedRow;
                if (flag == true)
                    CloseConnection();  
            }
            catch (Exception ex)
            {
                classUpdatePhoneLogWriter.WriteLog("Error Update_DISABLE_CardReplacement() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }

        }
        public int Update_DIS_Rep_Renew_up_dow(string pan, string user, string date_time)
        {
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_IM");// 
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_IM");
                //OpenConnection("CW_IM_UAT");//hhhh 
                OracleCommand cmd = new OracleCommand("Update_DIS_Rep_Renew_up_dow", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter panP = new OracleParameter("pan_p", pan);
                panP.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(panP);

                OracleParameter user_p = new OracleParameter("user_p", user);
                user_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(user_p);

                OracleParameter date_p = new OracleParameter("date_p", date_time);
                date_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(date_p);

                OracleString rowID;
                int insertedRow = 0;
                
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
                
            }
            catch (Exception ex)
            {
                classUpdatePhoneLogWriter.WriteLog("Error Update_DISABLE_CardReplacement() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();   
                return 0;
            }
            
        }
        public int Update_Status_SMS_CPGD(
        string pan,
        string sms_date,//long sms_date,
        string UPDATE_DATETIME,
        string sms_stat
        )
        {
            bool flag = false;
            try
            {
                flag = OpenConnection("CW_DW");
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("fpt.Update_Status_SMS_CPGD", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                OracleParameter sms_date_p = new OracleParameter("sms_date_p", sms_date);
                sms_date_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_date_p);

                OracleParameter UPDATE_DATETIME_P = new OracleParameter("UPDATE_DATETIME_P", UPDATE_DATETIME);
                UPDATE_DATETIME_P.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(UPDATE_DATETIME_P);

                OracleParameter sms_stat_p = new OracleParameter("sms_stat_p", sms_stat);
                sms_stat_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_stat_p);

                OracleString rowID;
                int updateRow = 0;
                
                updateRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return updateRow;
            }
            catch (Exception ex)
            {
                classCapPhepGDLogWriter.WriteLog("Error Update_Status_SMS_CPGD() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }
            //CloseConnection();// dong ket noi CSDL   
        }
        public int Update_Status_SMS(
        string pan,
        string sms_date,//long sms_date,
        string approve_code,
        string sms_stat 
        )
        {
            bool flag = false;
             try
            {
                flag = OpenConnection("CW_DW");
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("fpt.Update_Status_SMS", connection);               
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);               

                OracleParameter sms_date_p = new OracleParameter("sms_date_p", sms_date);
                sms_date_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_date_p);
               
                OracleParameter approve_code_p = new OracleParameter("approve_code_p", approve_code);
                approve_code_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(approve_code_p);

                OracleParameter sms_stat_p = new OracleParameter("sms_stat_p", sms_stat);
                sms_stat_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_stat_p);                

                OracleString rowID;
                int updateRow = 0;
                
                updateRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return updateRow;
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error Update_Status_SMS() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }
            //CloseConnection();// dong ket noi CSDL   
        }
        public int InsertOutBalChangeMDSMSToDW(
                                            string sms_type,
                                            string sms_detail,
                                            string dest_mobile,//long dest_mobile,
                                            DateTime get_trans_datetime,
                                            string pan,
                                            string card_brn,
                                            string card_type,
                                            string sms_date,//long sms_date,
                                            string sms_time,//long sms_time,
                                            double trans_amt,//long trans_amt
                                            string crncy_cde,//int crncy_cde,
                                            string merc_name,
                                            string approve_code,
                                            string trans_type,
                                            string trans_status,
                                            string update_datetime,
                                            string sms_stat                                            
                                            )
        {
            bool flag = false;
            try
            {
                flag = OpenConnection("CW_DW");// mo ket noi CSDL     
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("fpt.Insert_Outstanding_Bal_Change2", connection);
                //OracleCommand cmd = new OracleCommand("fpt.Insert_Outstanding_Bal_Change3", connection);//hhhh test insert han muc

                //OracleCommand cmd = new OracleCommand("fpt.Insert_Outstanding_Bal_Change", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", sms_type);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                OracleParameter sms_detail_p = new OracleParameter("sms_detail_p", sms_detail);
                sms_detail_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_detail_p);

                OracleParameter dest_mobile_p = new OracleParameter("dest_mobile_p", dest_mobile);
                dest_mobile_p.Direction = ParameterDirection.Input;
                //dest_mobile_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(dest_mobile_p);

                OracleParameter get_trans_datetime_p = new OracleParameter("get_trans_datetime_p", get_trans_datetime);
                get_trans_datetime_p.Direction = ParameterDirection.Input;
                //get_trans_datetime_p.OracleType = OracleType.DateTime;
                cmd.Parameters.Add(get_trans_datetime_p);

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                OracleParameter card_brn_p = new OracleParameter("card_brn_p", card_brn);
                card_brn_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_brn_p);

                OracleParameter card_type_p = new OracleParameter("card_type_p", card_type);
                card_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_type_p);

                OracleParameter sms_date_p = new OracleParameter("sms_date_p", sms_date);
                sms_date_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_date_p);

                OracleParameter sms_time_p = new OracleParameter("sms_time_p", sms_time);
                sms_time_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_time_p);

                OracleParameter trans_amt_p = new OracleParameter("trans_amt_p", trans_amt);
                trans_amt_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(trans_amt_p);

                OracleParameter crncy_cde_p = new OracleParameter("crncy_cde_p", crncy_cde);
                crncy_cde_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(crncy_cde_p);

                OracleParameter merc_name_p = new OracleParameter("merc_name_p", merc_name);
                merc_name_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(merc_name_p);

                OracleParameter approve_code_p = new OracleParameter("approve_code_p", approve_code);
                approve_code_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(approve_code_p);

                OracleParameter trans_type_p = new OracleParameter("trans_type_p", trans_type);
                trans_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(trans_type_p);

                OracleParameter trans_status_p = new OracleParameter("trans_status_p", trans_status);
                trans_status_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(trans_status_p);

                OracleParameter update_datetime_p = new OracleParameter("update_datetime_p", update_datetime);
                update_datetime_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(update_datetime_p);

                OracleParameter sms_stat_p = new OracleParameter("sms_stat_p", sms_stat);
                sms_stat_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_stat_p);               

                OracleString rowID;
                int insertedRow = 0;

                //classOutstandingChangeLogWriter.WriteLog("date time: ," + get_trans_datetime);
                
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error InsertOutBalChangeMDSMSToDW() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }
            //CloseConnection();// dong ket noi CSDL   

        }

        public int InsertOutBalChangeSMSToDW(
                                            string sms_type,
                                            string sms_detail,
                                            string dest_mobile,//long dest_mobile,
                                            DateTime get_trans_datetime,
                                            string pan,
                                            string card_brn,
                                            string card_type,
                                            string sms_date,//long sms_date,
                                            string sms_time,//long sms_time,
                                            double trans_amt,//long trans_amt
                                            string crncy_cde,//int crncy_cde,
                                            string merc_name,
                                            string approve_code,
                                            string trans_type,
                                            string trans_status,
                                            string update_datetime,
                                            string sms_stat,
                                            string  CLOSING_BALANCE,
                                            string UNPOST_AMT,
                                            string CASH_ADVANCE,
                                            string HOLD_LIMIT,
                                            string TOT_LMT,
                                            string MSL_AVAILABLE,
                                            string CARD_AVAILABLE
                                            )
        {
            bool flag = false;
            try
            {
                flag = OpenConnection("CW_DW");// mo ket noi CSDL   
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                //OracleCommand cmd = new OracleCommand("fpt.Insert_Outstanding_Bal_Change2", connection);
                //OracleCommand cmd = new OracleCommand("fpt.Insert_Outstanding_Bal_Change3", connection);//hhhh test insert han muc

                OracleCommand cmd = new OracleCommand("fpt.Insert_Outstanding_Bal_Change", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", sms_type);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                OracleParameter sms_detail_p = new OracleParameter("sms_detail_p", sms_detail);
                sms_detail_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_detail_p);

                OracleParameter dest_mobile_p = new OracleParameter("dest_mobile_p", dest_mobile);
                dest_mobile_p.Direction = ParameterDirection.Input;
                //dest_mobile_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(dest_mobile_p);

                OracleParameter get_trans_datetime_p = new OracleParameter("get_trans_datetime_p", get_trans_datetime);
                get_trans_datetime_p.Direction = ParameterDirection.Input;
                //get_trans_datetime_p.OracleType = OracleType.DateTime;
                cmd.Parameters.Add(get_trans_datetime_p);

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                OracleParameter card_brn_p = new OracleParameter("card_brn_p", card_brn);
                card_brn_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_brn_p);

                OracleParameter card_type_p = new OracleParameter("card_type_p", card_type);
                card_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_type_p);

                OracleParameter sms_date_p = new OracleParameter("sms_date_p", sms_date);
                sms_date_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_date_p);

                OracleParameter sms_time_p = new OracleParameter("sms_time_p", sms_time);
                sms_time_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_time_p);

                OracleParameter trans_amt_p = new OracleParameter("trans_amt_p", trans_amt);
                trans_amt_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(trans_amt_p);

                OracleParameter crncy_cde_p = new OracleParameter("crncy_cde_p", crncy_cde);
                crncy_cde_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(crncy_cde_p);

                OracleParameter merc_name_p = new OracleParameter("merc_name_p", merc_name);
                merc_name_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(merc_name_p);

                OracleParameter approve_code_p = new OracleParameter("approve_code_p", approve_code);
                approve_code_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(approve_code_p);

                OracleParameter trans_type_p = new OracleParameter("trans_type_p", trans_type);
                trans_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(trans_type_p);

                OracleParameter trans_status_p = new OracleParameter("trans_status_p", trans_status);
                trans_status_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(trans_status_p);
               
                OracleParameter update_datetime_p = new OracleParameter("update_datetime_p", update_datetime);
                update_datetime_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(update_datetime_p);

                OracleParameter sms_stat_p = new OracleParameter("sms_stat_p", sms_stat);
                sms_stat_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_stat_p);

                OracleParameter cls_bal_p = new OracleParameter("cls_bal_p", CLOSING_BALANCE);
                cls_bal_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(cls_bal_p);

                OracleParameter UNPOST_AMT_P = new OracleParameter("UNPOST_AMT_P", UNPOST_AMT);
                UNPOST_AMT_P.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(UNPOST_AMT_P);

                OracleParameter CASH_ADVANCE_P = new OracleParameter("CASH_ADVANCE_P", CASH_ADVANCE);
                CASH_ADVANCE_P.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(CASH_ADVANCE_P);

                OracleParameter HOLD_LIMIT_P = new OracleParameter("HOLD_LIMIT_P", HOLD_LIMIT);
                HOLD_LIMIT_P.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(HOLD_LIMIT_P);

                OracleParameter TOT_LMT_P = new OracleParameter("TOT_LMT_P", TOT_LMT);
                TOT_LMT_P.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(TOT_LMT_P);

                OracleParameter MSL_AVAILABLE_P = new OracleParameter("MSL_AVAILABLE_P", MSL_AVAILABLE);
                MSL_AVAILABLE_P.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(MSL_AVAILABLE_P);

                OracleParameter CARD_AVAILABLE_P = new OracleParameter("CARD_AVAILABLE_P", CARD_AVAILABLE);
                CARD_AVAILABLE_P.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(CARD_AVAILABLE_P); 

                OracleString rowID;
                int insertedRow = 0;

                //classOutstandingChangeLogWriter.WriteLog("date time: ," + get_trans_datetime);
                
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
            }
            catch (Exception ex)
            {
                classOutstandingChangeLogWriter.WriteLog("Error InsertOutBalChangeSMSToDW() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }
            //CloseConnection();// dong ket noi CSDL   
            
        }


        public int InsertNoQuaHanSMSToDW(string sms_type,
                                            string sms_detail,
                                            string dest_mobile,
                                            DateTime get_trans_datetime,                                    
                                            string pan,
                                            string card_brn,                   
                                            string update_datetime,
                                            string sms_stat
                                            )
        {
            bool flag = false;
            try
            {
              
                flag=OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");// mo ket noi CSDL
                OracleCommand cmd = new OracleCommand("fpt.Insert_NoQuaHan", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", sms_type);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);


                OracleParameter sms_detail_p = new OracleParameter("sms_detail_p", sms_detail);
                sms_detail_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_detail_p);

                OracleParameter dest_mobile_p = new OracleParameter("dest_mobile_p", dest_mobile);
                dest_mobile_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(dest_mobile_p);

                OracleParameter get_trans_datetime_p = new OracleParameter("get_trans_datetime_p", get_trans_datetime);
                get_trans_datetime_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(get_trans_datetime_p);

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                OracleParameter card_brn_p = new OracleParameter("card_brn_p", card_brn);
                card_brn_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_brn_p);

                         
                OracleParameter update_datetime_p = new OracleParameter("update_datetime_p", update_datetime);
                update_datetime_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(update_datetime_p);

                OracleParameter sms_stat_p = new OracleParameter("sms_stat_p", sms_stat);
                sms_stat_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_stat_p);

                OracleString rowID;
                int insertedRow = 0;

                
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;

            }
            catch (Exception ex)
            {
                classNoQuaHanLogWriter.WriteLog("Error InsertNoQuaHanSMSToDW() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }
           
        }
        public int InsertKichHoatTheSMSToDW(string sms_type,
                                            string sms_detail,
                                            string dest_mobile,
                                            DateTime get_trans_datetime,
                                            string pan,
                                            string card_brn,
                                            string card_type,
                                            string sms_date,
                                            string sms_time,                                           
                                            string update_datetime,
                                            string sms_stat
                                            )
        {
            bool flag = false;
            try
            {
                flag = OpenConnection("CW_DW");
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                //OracleCommand cmd = new OracleCommand("fpt.Insert_KichHoatThe", connection);
                OracleCommand cmd = new OracleCommand("fpt.Insert_KichHoatThe2", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", sms_type);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);


                OracleParameter sms_detail_p = new OracleParameter("sms_detail_p", sms_detail);
                sms_detail_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_detail_p);

                OracleParameter dest_mobile_p = new OracleParameter("dest_mobile_p", dest_mobile);
                dest_mobile_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(dest_mobile_p);

                OracleParameter get_trans_datetime_p = new OracleParameter("get_trans_datetime_p", get_trans_datetime);
                get_trans_datetime_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(get_trans_datetime_p);

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                OracleParameter card_brn_p = new OracleParameter("card_brn_p", card_brn);
                card_brn_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_brn_p);

                OracleParameter card_type_p = new OracleParameter("card_type_p", card_type);
                card_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_type_p);

                OracleParameter sms_date_p = new OracleParameter("sms_date_p", sms_date);
                sms_date_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_date_p);
                
                OracleParameter sms_time_p = new OracleParameter("sms_time_p", sms_time);
                sms_time_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_time_p);
             
                OracleParameter update_datetime_p = new OracleParameter("update_datetime_p", update_datetime);
                update_datetime_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(update_datetime_p);

                OracleParameter sms_stat_p = new OracleParameter("sms_stat_p", sms_stat);
                sms_stat_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_stat_p);             

                OracleString rowID;
                int insertedRow = 0;

               
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
            }
            catch (Exception ex)
            {
                classKichHoatTheLogWriter.WriteLog("Error InsertKichHoatTheSMSToDW(), " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }
            
        }
        public int InsertBlockAndFailAnnualFeeSMSToDW(
                                          string sms_type,
                                          string sms_detail,
                                          string dest_mobile,//long dest_mobile,
                                          DateTime get_trans_datetime,
                                          string pan,
                                          string card_brn,
                                          string card_type,
                                          string sms_date,//long sms_date,
                                          string sms_time,//long sms_time,
                                          string trans_amt,//long trans_amt,
            //string crncy_cde,//int crncy_cde,
            //string merc_name,
            //string approve_code,
            //string trans_type,
                                          string update_datetime,
                                          string sms_stat
                                          )
        {
            bool flag = false;
            try
            {
                flag = OpenConnection("CW_DW"); ;// mo ket noi CSDL  
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("fpt.Insert_BlockAndFailAnnualFee", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", sms_type);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                OracleParameter sms_detail_p = new OracleParameter("sms_detail_p", sms_detail);
                sms_detail_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_detail_p);

                OracleParameter dest_mobile_p = new OracleParameter("dest_mobile_p", dest_mobile);
                dest_mobile_p.Direction = ParameterDirection.Input;
                //dest_mobile_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(dest_mobile_p);

                OracleParameter get_trans_datetime_p = new OracleParameter("get_trans_datetime_p", get_trans_datetime);
                get_trans_datetime_p.Direction = ParameterDirection.Input;
                //get_trans_datetime_p.OracleType = OracleType.DateTime;
                cmd.Parameters.Add(get_trans_datetime_p);

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                OracleParameter card_brn_p = new OracleParameter("card_brn_p", card_brn);
                card_brn_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_brn_p);

                OracleParameter card_type_p = new OracleParameter("card_type_p", card_type);
                card_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_type_p);

                OracleParameter sms_date_p = new OracleParameter("sms_date_p", sms_date);
                sms_date_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_date_p);

                OracleParameter sms_time_p = new OracleParameter("sms_time_p", sms_time);
                sms_time_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_time_p);

                OracleParameter trans_amt_p = new OracleParameter("trans_amt_p", trans_amt);
                trans_amt_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(trans_amt_p);

                //OracleParameter crncy_cde_p = new OracleParameter("crncy_cde_p", crncy_cde);
                //crncy_cde_p.Direction = ParameterDirection.Input;
                ////sms_date_p.OracleType = OracleType.Number;
                //cmd.Parameters.Add(crncy_cde_p);

                //OracleParameter merc_name_p = new OracleParameter("merc_name_p", merc_name);
                //merc_name_p.Direction = ParameterDirection.Input;
                //cmd.Parameters.Add(merc_name_p);

                //OracleParameter approve_code_p = new OracleParameter("approve_code_p", approve_code);
                //approve_code_p.Direction = ParameterDirection.Input;
                //cmd.Parameters.Add(approve_code_p);

                //OracleParameter trans_type_p = new OracleParameter("trans_type_p", trans_type);
                //trans_type_p.Direction = ParameterDirection.Input;
                //cmd.Parameters.Add(trans_type_p);

                OracleParameter update_datetime_p = new OracleParameter("update_datetime_p", update_datetime);
                update_datetime_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(update_datetime_p);

                OracleParameter sms_stat_p = new OracleParameter("sms_stat_p", sms_stat);
                sms_stat_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_stat_p);

                OracleString rowID;
                int insertedRow = 0;

                
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
            }
            catch (Exception ex)
            {

                classAnnual_FeeLogWriter.WriteLog("Error InsertAnnualFeeSMSToDW()at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }
            //CloseConnection();// dong ket noi CSDL   

        }
        public int InsertAnnualFeeSMSToDW(
                                           string sms_type,
                                           string sms_detail,
                                           string dest_mobile,//long dest_mobile,
                                           DateTime get_trans_datetime,
                                           string pan,
                                           string card_brn,
                                           string card_type,
                                           string sms_date,//long sms_date,
                                           string sms_time,//long sms_time,
                                           double trans_amt,//long trans_amt,
                                           //string crncy_cde,//int crncy_cde,
                                           //string merc_name,
                                           //string approve_code,
                                           //string trans_type,
                                           string update_datetime,
                                           string sms_stat
                                           )
        {
            bool flag = false;
            try
            {
                flag = OpenConnection("CW_DW");// mo ket noi CSDL    
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("fpt.Insert_PhiThuongNien", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", sms_type);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                OracleParameter sms_detail_p = new OracleParameter("sms_detail_p", sms_detail);
                sms_detail_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_detail_p);

                OracleParameter dest_mobile_p = new OracleParameter("dest_mobile_p", dest_mobile);
                dest_mobile_p.Direction = ParameterDirection.Input;
                //dest_mobile_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(dest_mobile_p);

                OracleParameter get_trans_datetime_p = new OracleParameter("get_trans_datetime_p", get_trans_datetime);
                get_trans_datetime_p.Direction = ParameterDirection.Input;
                //get_trans_datetime_p.OracleType = OracleType.DateTime;
                cmd.Parameters.Add(get_trans_datetime_p);

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                OracleParameter card_brn_p = new OracleParameter("card_brn_p", card_brn);
                card_brn_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_brn_p);

                OracleParameter card_type_p = new OracleParameter("card_type_p", card_type);
                card_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_type_p);

                OracleParameter sms_date_p = new OracleParameter("sms_date_p", sms_date);
                sms_date_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_date_p);

                OracleParameter sms_time_p = new OracleParameter("sms_time_p", sms_time);
                sms_time_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_time_p);

                OracleParameter trans_amt_p = new OracleParameter("trans_amt_p", trans_amt);
                trans_amt_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(trans_amt_p);

                //OracleParameter crncy_cde_p = new OracleParameter("crncy_cde_p", crncy_cde);
                //crncy_cde_p.Direction = ParameterDirection.Input;
                ////sms_date_p.OracleType = OracleType.Number;
                //cmd.Parameters.Add(crncy_cde_p);

                //OracleParameter merc_name_p = new OracleParameter("merc_name_p", merc_name);
                //merc_name_p.Direction = ParameterDirection.Input;
                //cmd.Parameters.Add(merc_name_p);

                //OracleParameter approve_code_p = new OracleParameter("approve_code_p", approve_code);
                //approve_code_p.Direction = ParameterDirection.Input;
                //cmd.Parameters.Add(approve_code_p);

                //OracleParameter trans_type_p = new OracleParameter("trans_type_p", trans_type);
                //trans_type_p.Direction = ParameterDirection.Input;
                //cmd.Parameters.Add(trans_type_p);

                OracleParameter update_datetime_p = new OracleParameter("update_datetime_p", update_datetime);
                update_datetime_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(update_datetime_p);

                OracleParameter sms_stat_p = new OracleParameter("sms_stat_p", sms_stat);
                sms_stat_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_stat_p);

                OracleString rowID;
                int insertedRow = 0;
                

                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
            }
            catch (Exception ex)
            {
                
                classAnnual_FeeLogWriter.WriteLog("Error InsertAnnualFeeSMSToDW()at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }
            //CloseConnection();// dong ket noi CSDL   

        }

       
        public int Insert_UPD_INF_AUD(string update_type, string cif, string dest_mobile, //string email, 
            string update_datetime, int proces_num)
        {
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_DW");//hhhh
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");

                OracleCommand cmd = new OracleCommand("fpt.Insert_UPD_INF_AUD", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter update_type_p = new OracleParameter("update_type_p", update_type);
                update_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(update_type_p);

                OracleParameter cif_p = new OracleParameter("dest_cif_p", cif);
                cif_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(cif_p);

                OracleParameter dest_mobile_p = new OracleParameter("dest_mobile_p", dest_mobile);
                dest_mobile_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(dest_mobile_p);

                //OracleParameter email_p = new OracleParameter("DEST_EMAIL_P", email);
                //email_p.Direction = ParameterDirection.Input;               
                //cmd.Parameters.Add(email_p);

                OracleParameter update_datetime_p = new OracleParameter("update_datetime_p", update_datetime);
                update_datetime_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(update_datetime_p);

                OracleParameter Num_Sec_p = new OracleParameter("Num_Sec_p", proces_num);
                Num_Sec_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(Num_Sec_p);

                OracleString rowID;
                int insertedRow = 0;
                
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();  
                return insertedRow;
            }
            catch (Exception ex)
            {
                classUpdatePhoneLogWriter.WriteLog("Error Insert_UPD_INF_AUD()at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();  
                return 0;
            }
            

        }
        public int Insert_Update_InfDW(string update_type, string cif, string dest_mobile, //string email, 
            string update_datetime, string act_typ)
        {
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_DW");//hhhh
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("fpt.Insert_Update_InfDW", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter update_type_p = new OracleParameter("update_type_p", update_type);
                update_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(update_type_p);

                OracleParameter cif_p = new OracleParameter("dest_cif_p", cif);
                cif_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(cif_p);

                OracleParameter dest_mobile_p = new OracleParameter("dest_mobile_p", dest_mobile);
                dest_mobile_p.Direction = ParameterDirection.Input;              
                cmd.Parameters.Add(dest_mobile_p);

                //OracleParameter email_p = new OracleParameter("DEST_EMAIL_P", email);
                //email_p.Direction = ParameterDirection.Input;              
                //cmd.Parameters.Add(email_p);

                OracleParameter update_datetime_p = new OracleParameter("update_datetime_p", update_datetime);
                update_datetime_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(update_datetime_p);

                OracleParameter act_typ_p = new OracleParameter("ACTION_TYPE_P", act_typ);
                act_typ_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(act_typ_p);

                OracleString rowID;
                int insertedRow = 0;
                
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
                 
            }
            catch (Exception ex)
            {
                classUpdatePhoneLogWriter.WriteLog("Error Insert_Update_InfDW()at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();   
                return 0;
            }
            

        }
        public int InsertGDTangTienSMSToDW(
                                           string sms_type,
                                           string sms_detail,
                                           string dest_mobile,//long dest_mobile,
                                           DateTime get_trans_datetime,
                                           string pan,
                                           string card_brn,
                                           string card_type,
                                           string sms_date,//long sms_date,
                                           string sms_time,//long sms_time,
                                           double trans_amt,//long trans_amt,
                                           string crncy_cde,//int crncy_cde,
                                           string merc_name,
                                           string approve_code,
                                           string trans_type,                                         
                                           string update_datetime,
                                           string sms_stat
                                           )
        {
            bool flag = false;
            try
            {
                flag = OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                //OracleCommand cmd = new OracleCommand("fpt.Insert_GDTangTien", connection);
                OracleCommand cmd = new OracleCommand("fpt.Insert_GDTangTien2", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", sms_type);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                OracleParameter sms_detail_p = new OracleParameter("sms_detail_p", sms_detail);
                sms_detail_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_detail_p);

                OracleParameter dest_mobile_p = new OracleParameter("dest_mobile_p", dest_mobile);
                dest_mobile_p.Direction = ParameterDirection.Input;
                //dest_mobile_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(dest_mobile_p);

                OracleParameter get_trans_datetime_p = new OracleParameter("get_trans_datetime_p", get_trans_datetime);
                get_trans_datetime_p.Direction = ParameterDirection.Input;
                //get_trans_datetime_p.OracleType = OracleType.DateTime;
                cmd.Parameters.Add(get_trans_datetime_p);

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                OracleParameter card_brn_p = new OracleParameter("card_brn_p", card_brn);
                card_brn_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_brn_p);

                OracleParameter card_type_p = new OracleParameter("card_type_p", card_type);
                card_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_type_p);

                OracleParameter sms_date_p = new OracleParameter("sms_date_p", sms_date);
                sms_date_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_date_p);

                OracleParameter sms_time_p = new OracleParameter("sms_time_p", sms_time);
                sms_time_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_time_p);

                OracleParameter trans_amt_p = new OracleParameter("trans_amt_p", trans_amt);
                trans_amt_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(trans_amt_p);

                OracleParameter crncy_cde_p = new OracleParameter("crncy_cde_p", crncy_cde);
                crncy_cde_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(crncy_cde_p);

                OracleParameter merc_name_p = new OracleParameter("merc_name_p", merc_name);
                merc_name_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(merc_name_p);

                OracleParameter approve_code_p = new OracleParameter("approve_code_p", approve_code);
                approve_code_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(approve_code_p);

                OracleParameter trans_type_p = new OracleParameter("trans_type_p", trans_type);
                trans_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(trans_type_p);
               
                OracleParameter update_datetime_p = new OracleParameter("update_datetime_p", update_datetime);
                update_datetime_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(update_datetime_p);

                OracleParameter sms_stat_p = new OracleParameter("sms_stat_p", sms_stat);
                sms_stat_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_stat_p);

                OracleString rowID;
                int insertedRow = 0;

                
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
            }
            catch (Exception ex)
            {
                classGDTangTienLogWriter.WriteLog("Error InsertGDTangTienSMSToDW()at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }
            //CloseConnection();// dong ket noi CSDL   
            
        }

        public int InsertThuNoTatToanIPPSMSToDW(
                                           string sms_type,
                                           string sms_detail,
                                           string dest_mobile,//long dest_mobile,
                                           DateTime get_trans_datetime,
                                           string pan,
                                           string card_brn,
                                           string card_type,
                                           string sms_date,//long sms_date,
                                           string sms_time,//long sms_time,
                                           string loc_bal,
                                           double paymented_amt,//long paymented_amt,
                                           string update_datetime,
                                           string sms_stat,
                                           string app_cde
                                           )
        {
            bool flag = false;
            try
            {
                flag = OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                //OracleCommand cmd = new OracleCommand("fpt.Insert_Outstanding_Pay_Manual2", connection);//11-08-17
                OracleCommand cmd = new OracleCommand("fpt.Insert_Outstanding_Pay_Manual", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", sms_type);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                OracleParameter sms_detail_p = new OracleParameter("sms_detail_p", sms_detail);
                sms_detail_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_detail_p);

                OracleParameter dest_mobile_p = new OracleParameter("dest_mobile_p", dest_mobile);
                dest_mobile_p.Direction = ParameterDirection.Input;
                //dest_mobile_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(dest_mobile_p);

                OracleParameter get_trans_datetime_p = new OracleParameter("get_trans_datetime_p", get_trans_datetime);
                get_trans_datetime_p.Direction = ParameterDirection.Input;
                //get_trans_datetime_p.OracleType = OracleType.DateTime;
                cmd.Parameters.Add(get_trans_datetime_p);

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                OracleParameter card_brn_p = new OracleParameter("card_brn_p", card_brn);
                card_brn_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_brn_p);

                OracleParameter card_type_p = new OracleParameter("card_type_p", card_type);
                card_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_type_p);

                OracleParameter sms_date_p = new OracleParameter("sms_date_p", sms_date);
                sms_date_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_date_p);

                OracleParameter sms_time_p = new OracleParameter("sms_time_p", sms_time);
                sms_time_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_time_p);

                OracleParameter loc_bal_p = new OracleParameter("loc_bal_p", loc_bal);
                loc_bal_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(loc_bal_p);

                OracleParameter paymented_amt_p = new OracleParameter("paymented_amt_p", paymented_amt);
                paymented_amt_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(paymented_amt_p);

                OracleParameter update_datetime_p = new OracleParameter("update_datetime_p", update_datetime);
                update_datetime_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(update_datetime_p);

                OracleParameter sms_stat_p = new OracleParameter("sms_stat_p", sms_stat);
                sms_stat_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_stat_p);

                OracleParameter app_cde_P = new OracleParameter("app_cde_P", app_cde);
                app_cde_P.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(app_cde_P);

                OracleString rowID;
                int insertedRow = 0;

               
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
            }
            catch (Exception ex)
            {
                classThuNoTatToanIPPLogWriter.WriteLog("Error InsertPaymnetManualSMSToDW() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }


        }
       

        public int InsertPaymnetManualSMSToDW(
                                            string sms_type,
                                            string sms_detail,
                                            string dest_mobile,//long dest_mobile,
                                            DateTime get_trans_datetime,
                                            string pan,
                                            string card_brn,
                                            string card_type,
                                            string sms_date,//long sms_date,
                                            string sms_time,//long sms_time,
                                            string loc_bal,
                                            double paymented_amt,//long paymented_amt,
                                            string update_datetime,
                                            string sms_stat,
                                            string app_cde,
                                            string UNPOST_AMT,
                                            string CASH_ADVANCE,
                                            string HOLD_LIMIT,
                                            string TOT_LMT,
                                            string MSL_AVAILABLE,
                                            string CARD_AVAILABLE
                                            
                                            )
        {
            bool flag = false;
            try
            {
                flag = OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("fpt.Insert_Outstanding_Pay_Manual2", connection);//11-08-17
                //OracleCommand cmd = new OracleCommand("fpt.Insert_Outstanding_Pay_Manual", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", sms_type);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                OracleParameter sms_detail_p = new OracleParameter("sms_detail_p", sms_detail);
                sms_detail_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_detail_p);

                OracleParameter dest_mobile_p = new OracleParameter("dest_mobile_p", dest_mobile);
                dest_mobile_p.Direction = ParameterDirection.Input;
                //dest_mobile_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(dest_mobile_p);

                OracleParameter get_trans_datetime_p = new OracleParameter("get_trans_datetime_p", get_trans_datetime);
                get_trans_datetime_p.Direction = ParameterDirection.Input;
                //get_trans_datetime_p.OracleType = OracleType.DateTime;
                cmd.Parameters.Add(get_trans_datetime_p);

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                OracleParameter card_brn_p = new OracleParameter("card_brn_p", card_brn);
                card_brn_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_brn_p);

                OracleParameter card_type_p = new OracleParameter("card_type_p", card_type);
                card_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_type_p);

                OracleParameter sms_date_p = new OracleParameter("sms_date_p", sms_date);
                sms_date_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_date_p);

                OracleParameter sms_time_p = new OracleParameter("sms_time_p", sms_time);
                sms_time_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_time_p);

                OracleParameter loc_bal_p = new OracleParameter("loc_bal_p", loc_bal);
                loc_bal_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(loc_bal_p);

                OracleParameter paymented_amt_p = new OracleParameter("paymented_amt_p", paymented_amt);
                paymented_amt_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(paymented_amt_p);

                OracleParameter update_datetime_p = new OracleParameter("update_datetime_p", update_datetime);
                update_datetime_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(update_datetime_p);

                OracleParameter sms_stat_p = new OracleParameter("sms_stat_p", sms_stat);
                sms_stat_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_stat_p);

                OracleParameter app_cde_P = new OracleParameter("app_cde_P", app_cde);
                app_cde_P.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(app_cde_P);

                OracleParameter UNPOST_AMT_P = new OracleParameter("UNPOST_AMT_P", UNPOST_AMT);
                UNPOST_AMT_P.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(UNPOST_AMT_P);               

                OracleParameter CASH_ADVANCE_P = new OracleParameter("CASH_ADVANCE_P", CASH_ADVANCE);
                CASH_ADVANCE_P.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(CASH_ADVANCE_P);

                OracleParameter HOLD_LIMIT_P = new OracleParameter("HOLD_LIMIT_P", HOLD_LIMIT);
                HOLD_LIMIT_P.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(HOLD_LIMIT_P);

                OracleParameter TOT_LMT_P = new OracleParameter("TOT_LMT_P", TOT_LMT);
                TOT_LMT_P.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(TOT_LMT_P);

                OracleParameter MSL_AVAILABLE_P = new OracleParameter("MSL_AVAILABLE_P", MSL_AVAILABLE);
                MSL_AVAILABLE_P.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(MSL_AVAILABLE_P);

                OracleParameter CARD_AVAILABLE_P = new OracleParameter("CARD_AVAILABLE_P", CARD_AVAILABLE);
                CARD_AVAILABLE_P.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(CARD_AVAILABLE_P);            

                OracleString rowID;
                int insertedRow = 0;

                
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
            }
            catch (Exception ex)
            {
                classOutstandingBalancePaymentManualLogWriter.WriteLog("Error InsertPaymnetManualSMSToDW() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }
            
            
        }
        public int InsertThuNoFailToDW(
                                            string sms_type,
                                            string sms_detail,
                                            string dest_mobile,//long dest_mobile,
                                            DateTime get_trans_datetime,
                                            string pan,
                                            string card_brn,
                                            string card_type,
                                            string expired_date,
                                            string sms_stat,
                                            string update_time
                                            )
        {
            bool flag = false;
            try
            {
                flag = OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("fpt.Insert_ThuNoFail", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", sms_type);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                OracleParameter sms_detail_p = new OracleParameter("sms_detail_p", sms_detail);
                sms_detail_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_detail_p);

                OracleParameter dest_mobile_p = new OracleParameter("dest_mobile_p", dest_mobile);
                dest_mobile_p.Direction = ParameterDirection.Input;
                //dest_mobile_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(dest_mobile_p);

                OracleParameter get_trans_datetime_p = new OracleParameter("get_trans_datetime_p", get_trans_datetime);
                get_trans_datetime_p.Direction = ParameterDirection.Input;
                //get_trans_datetime_p.OracleType = OracleType.DateTime;
                cmd.Parameters.Add(get_trans_datetime_p);

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                OracleParameter card_brn_p = new OracleParameter("card_brn_p", card_brn);
                card_brn_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_brn_p);

                OracleParameter card_type_p = new OracleParameter("card_type_p", card_type);
                card_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_type_p);

                OracleParameter expired_date_p = new OracleParameter("expired_date_p", expired_date);
                expired_date_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(expired_date_p);

                OracleParameter sms_stat_p = new OracleParameter("sms_stat_p", sms_stat);
                sms_stat_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_stat_p);

                OracleParameter update_time_p = new OracleParameter("update_time", update_time);
                update_time_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(update_time_p);

                OracleString rowID;
                int insertedRow = 0;

                
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
            }
            catch (Exception ex)
            {
                classThuNoFailLogWriter.WriteLog("Error InsertThuNoFailToDW(), " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }


        }

        public int InsertExpiredExtensionToDW(
                                            string sms_type,
                                            string sms_detail,
                                            string dest_mobile,//long dest_mobile,
                                            DateTime get_trans_datetime,
                                            string pan,
                                            string card_brn,
                                            string card_type,
                                            string expired_date,
                                            string sms_stat
                                            )
        {
            bool flag = false;
            try
            {
                flag = OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("fpt.Insert_EXPIRED_DATE2", connection);
                //OracleCommand cmd = new OracleCommand("fpt.Insert_EXPIRED_DATE", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", sms_type);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                OracleParameter sms_detail_p = new OracleParameter("sms_detail_p", sms_detail);
                sms_detail_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_detail_p);

                OracleParameter dest_mobile_p = new OracleParameter("dest_mobile_p", dest_mobile);
                dest_mobile_p.Direction = ParameterDirection.Input;
                //dest_mobile_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(dest_mobile_p);

                OracleParameter get_trans_datetime_p = new OracleParameter("get_trans_datetime_p", get_trans_datetime);
                get_trans_datetime_p.Direction = ParameterDirection.Input;
                //get_trans_datetime_p.OracleType = OracleType.DateTime;
                cmd.Parameters.Add(get_trans_datetime_p);

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                OracleParameter card_brn_p = new OracleParameter("card_brn_p", card_brn);
                card_brn_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_brn_p);

                OracleParameter card_type_p = new OracleParameter("card_type_p", card_type);
                card_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_type_p);

                OracleParameter expired_date_p = new OracleParameter("expired_date_p", expired_date);
                expired_date_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(expired_date_p);

                OracleParameter sms_stat_p = new OracleParameter("sms_stat_p", sms_stat);
                sms_stat_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_stat_p);   

                OracleString rowID;
                int insertedRow = 0;

                
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
            }
            catch (Exception ex)
            {
                classExpiredExtensionLogWriter.WriteLog("Error InsertExpiredExtensionToDW(), " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }
         
            
        }

        public int InsertPaymnetAutoSMSToDW(
                                            string sms_type,
                                            string sms_detail,
                                            string dest_mobile,//long dest_mobile,
                                            DateTime get_trans_datetime,
                                            string pan,
                                            string card_brn,
                                            string card_type,
                                            string sms_date,//long sms_date,
                                            string sms_time,//long sms_time,
                                            string loc_bal,
                                            double paymented_amt,//long paymented_amt,
                                            string update_datetime,
                                            string sms_stat
                                            )
        {
            bool flag = false;
            try
            {
                flag = OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("fpt.Insert_Outstanding_Pay_Auto2", connection);
                //OracleCommand cmd = new OracleCommand("fpt.Insert_Outstanding_Pay_Auto", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", sms_type);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                OracleParameter sms_detail_p = new OracleParameter("sms_detail_p", sms_detail);
                sms_detail_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_detail_p);

                OracleParameter dest_mobile_p = new OracleParameter("dest_mobile_p", dest_mobile);
                dest_mobile_p.Direction = ParameterDirection.Input;
                //dest_mobile_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(dest_mobile_p);

                OracleParameter get_trans_datetime_p = new OracleParameter("get_trans_datetime_p", get_trans_datetime);
                get_trans_datetime_p.Direction = ParameterDirection.Input;
                //get_trans_datetime_p.OracleType = OracleType.DateTime;
                cmd.Parameters.Add(get_trans_datetime_p);

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                OracleParameter card_brn_p = new OracleParameter("card_brn_p", card_brn);
                card_brn_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_brn_p);

                OracleParameter card_type_p = new OracleParameter("card_type_p", card_type);
                card_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_type_p);

                OracleParameter sms_date_p = new OracleParameter("sms_date_p", sms_date);
                sms_date_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_date_p);

                OracleParameter sms_time_p = new OracleParameter("sms_time_p", sms_time);
                sms_time_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_time_p);

                OracleParameter loc_bal_p = new OracleParameter("loc_bal_p", loc_bal);
                loc_bal_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(loc_bal_p);

                OracleParameter paymented_amt_p = new OracleParameter("paymented_amt_p", paymented_amt);
                paymented_amt_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(paymented_amt_p);

                OracleParameter update_datetime_p = new OracleParameter("update_datetime_p", update_datetime);
                update_datetime_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(update_datetime_p);

                OracleParameter sms_stat_p = new OracleParameter("sms_stat_p", sms_stat);
                sms_stat_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_stat_p);   


                OracleString rowID;
                int insertedRow = 0;
                
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
            }
            catch (Exception ex)
            {
                classOutstandingBalancePaymentAutoLogWriter.WriteLog("Error InsertPaymnetAutoSMSToDW() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }
       
            
        }


        public int InsertReminderPayment_1SMSToDW(
                                            string sms_type,
                                            string sms_detail,
                                            string dest_mobile,//long dest_mobile,
                                            DateTime get_trans_datetime,
                                            string pan,
                                            string card_brn,
                                            string card_type,
                                            string sms_date,//long sms_date,
                                            //string sms_time,//long sms_time,
                                            string closing_balance,
                                            double minimum_payment,//long paymented_amt,
                                            string due_date,
                                            string sms_stat
                                            )
        {
            bool flag = false;
            try
            {
                flag = OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("fpt.Insert_Reminder_Payment_1_2", connection);
                //OracleCommand cmd = new OracleCommand("fpt.Insert_Reminder_Payment_1", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", sms_type);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                OracleParameter sms_detail_p = new OracleParameter("sms_detail_p", sms_detail);
                sms_detail_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_detail_p);

                OracleParameter dest_mobile_p = new OracleParameter("dest_mobile_p", dest_mobile);
                dest_mobile_p.Direction = ParameterDirection.Input;
                //dest_mobile_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(dest_mobile_p);

                OracleParameter get_trans_datetime_p = new OracleParameter("get_trans_datetime_p", get_trans_datetime);
                get_trans_datetime_p.Direction = ParameterDirection.Input;
                //get_trans_datetime_p.OracleType = OracleType.DateTime;
                cmd.Parameters.Add(get_trans_datetime_p);

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                OracleParameter card_brn_p = new OracleParameter("card_brn_p", card_brn);
                card_brn_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_brn_p);

                OracleParameter card_type_p = new OracleParameter("card_type_p", card_type);
                card_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_type_p);

                OracleParameter sms_date_p = new OracleParameter("sms_date_p", sms_date);
                sms_date_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_date_p);

                OracleParameter closing_balance_p = new OracleParameter("closing_balance_p", closing_balance);
                closing_balance_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(closing_balance_p);

                OracleParameter minimum_payment_p = new OracleParameter("minimum_payment_p", minimum_payment);
                minimum_payment_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(minimum_payment_p);
                
                OracleParameter due_date_p = new OracleParameter("due_date_p", due_date);
                due_date_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(due_date_p);

                OracleParameter sms_stat_p = new OracleParameter("sms_stat_p", sms_stat);
                sms_stat_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_stat_p);   

                OracleString rowID;
                int insertedRow = 0;
                
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
            }
            catch (Exception ex)
            {
                classReminderPayment1LogWriter.WriteLog("Error InsertReminderPayment_1SMSToDW()at DataAccess" + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }
         
            
        }

        public int InsertReminderPayment_2SMSToDW(
                                            string sms_type,
                                            string sms_detail,
                                            string dest_mobile,//long dest_mobile,
                                            DateTime get_trans_datetime,
                                            string pan,
                                            string card_brn,
                                            string card_type,
                                            string sms_date,//long sms_date,
                                            string closing_balance,
                                            string minimum_payment,//long paymented_amt,
                                            string due_date,
                                            string sms_stat
                                            )
        {
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("fpt.Insert_Reminder_Payment_1_2", connection);
                //OracleCommand cmd = new OracleCommand("fpt.Insert_Reminder_Payment_1", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", sms_type);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                OracleParameter sms_detail_p = new OracleParameter("sms_detail_p", sms_detail);
                sms_detail_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_detail_p);

                OracleParameter dest_mobile_p = new OracleParameter("dest_mobile_p", dest_mobile);
                dest_mobile_p.Direction = ParameterDirection.Input;
                //dest_mobile_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(dest_mobile_p);

                OracleParameter get_trans_datetime_p = new OracleParameter("get_trans_datetime_p", get_trans_datetime);
                get_trans_datetime_p.Direction = ParameterDirection.Input;
                //get_trans_datetime_p.OracleType = OracleType.DateTime;
                cmd.Parameters.Add(get_trans_datetime_p);

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                OracleParameter card_brn_p = new OracleParameter("card_brn_p", card_brn);
                card_brn_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_brn_p);

                OracleParameter card_type_p = new OracleParameter("card_type_p", card_type);
                card_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_type_p);

                OracleParameter sms_date_p = new OracleParameter("sms_date_p", sms_date);
                sms_date_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_date_p);

                OracleParameter closing_balance_p = new OracleParameter("closing_balance_p", closing_balance);
                closing_balance_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(closing_balance_p);

                OracleParameter minimum_payment_p = new OracleParameter("minimum_payment_p", minimum_payment);
                minimum_payment_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(minimum_payment_p);

                OracleParameter due_date_p = new OracleParameter("due_date_p", due_date);
                due_date_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(due_date_p);

                OracleParameter sms_stat_p = new OracleParameter("sms_stat_p", sms_stat);
                sms_stat_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_stat_p);

                OracleString rowID;
                int insertedRow = 0;
               
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
            }
            catch (Exception ex)
            {
                classReminderPayment2LogWriter.WriteLog("Error InsertReminderPayment_2SMSToDW() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }
        
            
        }

        public int Insert_MASTERCARD_EMAIL(
                                            string hoten,
                                            string email,
                                            double duno,
                                            double sotientt,
                                            double songaytrett,
                                            string loaithe                                                             
                                            )
        {
            bool flag = false;
            try
            {
                flag=OpenConnection("CW_DW");// mo ket noi CSDL                
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("fpt.Insert_MASTERCARD_EMAIL", connection);
                //OracleCommand cmd = new OracleCommand("FPT.Insert_MASTERCARD_EMAIL_T", connection);//hhhh

                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter hoten_p = new OracleParameter("hoten_p", hoten);
                hoten_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(hoten_p);

                OracleParameter mail_p = new OracleParameter("email_p", email);
                mail_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(mail_p);

                OracleParameter duno_p = new OracleParameter("duno_p", duno);
                duno_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(duno_p);

                OracleParameter sotien_p = new OracleParameter("sotientt_p", sotientt);
                sotien_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sotien_p);

                OracleParameter songaytrehan_p = new OracleParameter("songaytrett_p", songaytrett);
                songaytrehan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(songaytrehan_p);

                OracleParameter loaithe_p = new OracleParameter("loaithe_p", loaithe);
                loaithe_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(loaithe_p);  

                OracleString rowID;
                int insertedRow = 0;
                
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if(flag==true)
                    CloseConnection();
                return insertedRow;
                
            }
            catch (Exception ex)
            {
                classNoQuaHanLogWriter.WriteLog("Error Insert_MASTERCARD_EMAIL() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }        
            
        }



        public int InsertRewardPointToDW(
                                            string sms_type,
                                            string sms_detail,
                                            string dest_mobile,//long dest_mobile,
                                            DateTime get_trans_datetime,
                                            string pan,
                                            string card_brn,
                                            string card_type,
                                            string award_point,
                                            string award_month,
                                            string sms_stat
                                            )
        {
            bool flag = false;
            try
            {
                flag = OpenConnection("CW_DW");// mo ket noi CSDL
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("fpt.Insert_GET_AWARD_POINT2", connection);
                //OracleCommand cmd = new OracleCommand("fpt.Insert_GET_AWARD_POINT", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", sms_type);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                OracleParameter sms_detail_p = new OracleParameter("sms_detail_p", sms_detail);
                sms_detail_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_detail_p);

                OracleParameter dest_mobile_p = new OracleParameter("dest_mobile_p", dest_mobile);
                dest_mobile_p.Direction = ParameterDirection.Input;
                //dest_mobile_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(dest_mobile_p);

                OracleParameter get_trans_datetime_p = new OracleParameter("get_trans_datetime_p", get_trans_datetime);
                get_trans_datetime_p.Direction = ParameterDirection.Input;
                //get_trans_datetime_p.OracleType = OracleType.DateTime;
                cmd.Parameters.Add(get_trans_datetime_p);

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                OracleParameter card_brn_p = new OracleParameter("card_brn_p", card_brn);
                card_brn_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_brn_p);

                OracleParameter card_type_p = new OracleParameter("card_type_p", card_type);
                card_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_type_p);

                OracleParameter award_point_p = new OracleParameter("award_point_p", award_point);
                award_point_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(award_point_p);

                OracleParameter sms_date_p = new OracleParameter("sms_date_p", award_month);
                sms_date_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_date_p);

                OracleParameter sms_stat_p = new OracleParameter("sms_stat_p", sms_stat);
                sms_stat_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_stat_p);


                OracleString rowID;
                int insertedRow = 0;
                
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
            }
            catch (Exception ex)
            {
                classAccumulativeRewardPointLogWriter.WriteLog("Error InsertRewardPointToDW() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }
            //CloseConnection();// dong ket noi CSDL   
           
        }

        public int InsertSMSMessateToEBankGW_2(string idAlert, string mobile, string message,
                                           char msgstat, string smsType)
        {
            bool flag = false;
            try
            {

                //flag = OpenConnection("EBANK_GW");// mo ket noi CSDL 1111
                OracleCommand cmd = new OracleCommand("SMS_SCB.PROC_INS_MASTERCARD_KM", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter idAlert_p = new OracleParameter("id_alert", idAlert);
                idAlert_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(idAlert_p);

                OracleParameter mobile_p = new OracleParameter("mobile", mobile);
                mobile_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(mobile_p);

                OracleParameter message_p = new OracleParameter("message", message);
                message_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(message_p);

                OracleParameter msgstat_p = new OracleParameter("msgstat", msgstat);
                msgstat_p.Direction = ParameterDirection.Input;
                msgstat_p.OracleType = OracleType.Char;
                cmd.Parameters.Add(msgstat_p);

                OracleParameter mc_sms_type_p = new OracleParameter("mc_sms_type", smsType);
                mc_sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(mc_sms_type_p);

                OracleString rowID;
                int insertedRow = 0;

                //if (connection.State == ConnectionState.Closed)
                //    flag = OpenConnection("EBANK_GW");

                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
            }
            catch (Exception ex)
            {
                classEBankGWLogWriter.WriteLog("Error InsertSMSMessateToEBankGW_2() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }


        }
        public int InsertSMSMessateToEBankGW(string idAlert,string mobile , string message,
                                            char msgstat, string smsType)
        {
            bool flag = false;
            try
            {

                //flag = OpenConnection("EBANK_GW");// mo ket noi CSDL
                OracleCommand cmd = new OracleCommand("sms_scb.PROC_INS_MASTERCARD_SMS", connection);              
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter idAlert_p = new OracleParameter("id_alert", idAlert);
                idAlert_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(idAlert_p);

                OracleParameter mobile_p = new OracleParameter("mobile", mobile);
                mobile_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(mobile_p);

                OracleParameter message_p = new OracleParameter("message", message);
                message_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(message_p);

                OracleParameter msgstat_p = new OracleParameter("msgstat", msgstat);
                msgstat_p.Direction = ParameterDirection.Input;
                msgstat_p.OracleType = OracleType.Char;
                cmd.Parameters.Add(msgstat_p);

                OracleParameter mc_sms_type_p = new OracleParameter("mc_sms_type", smsType);
                mc_sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(mc_sms_type_p);

                OracleString rowID;
                int insertedRow = 0;

                //if (connection.State == ConnectionState.Closed)
                //    flag = OpenConnection("EBANK_GW");

                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
            }
            catch (Exception ex)
            {
                classEBankGWLogWriter.WriteLog("Error InsertSMSMessateToEBankGW() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }
     
            
        }

        public OracleCommand AddProcedureParameterToEBankGW()
        {
            OracleCommand cmd = new OracleCommand();
            bool flag = false;
            try
            {
                flag = OpenConnection("EBANK_GW");// mo ket noi CSDL
                cmd = new OracleCommand("sms_scb.PROC_INS_MASTERCARD_SMS", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter id_alert = new OracleParameter("id_alert", OracleType.VarChar, 50);
                id_alert.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(id_alert);

                OracleParameter mobile_p = new OracleParameter("mobile", OracleType.VarChar, 20);
                mobile_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(mobile_p);

                OracleParameter message_p = new OracleParameter("message", OracleType.VarChar, 160);
                message_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(message_p);

                OracleParameter msgstat_p = new OracleParameter("msgstat", OracleType.Char);
                msgstat_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(msgstat_p);

                OracleParameter mc_sms_type_p = new OracleParameter("mc_sms_type", OracleType.VarChar, 6);
                mc_sms_type_p.Direction = ParameterDirection.Input;
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("EBANK_GW");
                cmd.Parameters.Add(mc_sms_type_p);
                if (flag == true)
                    CloseConnection();
            }
            catch (Exception ex)
            {
                classEBankGWLogWriter.WriteLog("Error AddProcedureParameterToEBankGW() at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
            }
            return cmd;
        }


        public int ExecuteProcedureToEBankGW(OracleCommand cmd, string idAlert, string mobile, string message,
                                            char msgstat, string smsType)
        {
            OracleString rowID;
            int insertedRow = 0;

            try
            {
                cmd.Parameters.AddWithValue("id_alert", idAlert);
                cmd.Parameters.AddWithValue("mobile", mobile);
                cmd.Parameters.AddWithValue("message", message);
                cmd.Parameters.AddWithValue("msgstat", msgstat);
                cmd.Parameters.AddWithValue("mc_sms_type", smsType);

                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
            }
            catch (Exception ex)
            {
                classEBankGWLogWriter.WriteLog("Error ExecuteProcedureToEBankGW() at DataAccess, " + ex.Message);
            }
         
            return insertedRow;
        }

        //public int CHECK_TXN_IPC_PENDING()
        //{
        //    try
        //    {
        //        int flag = 0;
        //        bool temp = OpenConnectionSQL();
        //        SqlCommand cmd = new SqlCommand("CHECK_TXN_PENDING", connectionSQL);
        //        cmd.CommandType = CommandType.StoredProcedure;
        //        cmd.Parameters.Clear();               

        //        SqlParameter sqlParam = new SqlParameter("@Result", SqlDbType.Int);
        //        sqlParam.Direction = ParameterDirection.Output;
        //        cmd.Parameters.Add(sqlParam);
        //        cmd.ExecuteNonQuery();

        //        flag = int.Parse(cmd.Parameters["@Result"].Value.ToString());
        //        CloseConnectionSQL();
        //        return flag;
        //    }
        //    catch (Exception ex)
        //    {
        //        classDataAccessLogWriter.WriteLog("Err Check_TXN_PENDING() at DataAccess" + ex.ToString());
        //        return 0;
        //    }

        //}
        //private static bool OpenConnectionSQL()
        //{
        //    try
        //    {
        //        string connectionString = System.Configuration.ConfigurationManager.AppSettings["SQLServerIPC"].ToString();
        //        connectionSQL = new SqlConnection(connectionString);
        //        connectionSQL.Open();
        //        classDataAccessLogWriter.WriteLog("Open connection SQL successful for " + connectionSQL.ConnectionString);
        //        return true;
        //    }
        //    catch (Exception ex)
        //    {
        //        classDataAccessLogWriter.WriteLog("Error OpenConnectionSQL() , " + ex.Message);
        //        return false;
        //    }

        //}
        //private static bool CloseConnectionSQL()
        //{
        //    try
        //    {
        //        connectionSQL.Close();
        //        classDataAccessLogWriter.WriteLog("Close connection SQL successful for " + connectionSQL.ConnectionString);
        //        return true;
        //    }
        //    catch (Exception ex)
        //    {
        //        classDataAccessLogWriter.WriteLog("Error CloseConnectionSQL(), " + ex.Message);
        //        return false;
        //    }
        //}
        public int InsertGD_SMS_ToDW(
                                          string sms_type,
                                          string sms_detail,
                                          string dest_mobile,//long dest_mobile,
                                          DateTime get_trans_datetime,
                                          string pan,
                                          string card_brn,
                                          string card_type,
                                          string sms_date,//long sms_date,
                                          string sms_time,//long sms_time,
                                          double trans_amt,//long trans_amt,
                                          string crncy_cde,//int crncy_cde,
                                          string merc_name,
                                          string approve_code,
                                          string trans_type,
                                          string update_datetime,
                                          string sms_stat
                                          )
        {
            bool flag = false;
            try
            {
                flag = OpenConnection("CW_DW");
                if (connection.State == ConnectionState.Closed)
                    flag = OpenConnection("CW_DW");
                OracleCommand cmd = new OracleCommand("fpt.Insert_GD_To_DW", connection);
                cmd.CommandType = CommandType.StoredProcedure;

                OracleParameter sms_type_p = new OracleParameter("sms_type_p", sms_type);
                sms_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_type_p);

                OracleParameter sms_detail_p = new OracleParameter("sms_detail_p", sms_detail);
                sms_detail_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_detail_p);

                OracleParameter dest_mobile_p = new OracleParameter("dest_mobile_p", dest_mobile);
                dest_mobile_p.Direction = ParameterDirection.Input;
                //dest_mobile_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(dest_mobile_p);

                OracleParameter get_trans_datetime_p = new OracleParameter("get_trans_datetime_p", get_trans_datetime);
                get_trans_datetime_p.Direction = ParameterDirection.Input;
                //get_trans_datetime_p.OracleType = OracleType.DateTime;
                cmd.Parameters.Add(get_trans_datetime_p);

                OracleParameter pan_p = new OracleParameter("pan_p", pan);
                pan_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(pan_p);

                OracleParameter card_brn_p = new OracleParameter("card_brn_p", card_brn);
                card_brn_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_brn_p);

                OracleParameter card_type_p = new OracleParameter("card_type_p", card_type);
                card_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(card_type_p);

                OracleParameter sms_date_p = new OracleParameter("sms_date_p", sms_date);
                sms_date_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_date_p);

                OracleParameter sms_time_p = new OracleParameter("sms_time_p", sms_time);
                sms_time_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(sms_time_p);

                OracleParameter trans_amt_p = new OracleParameter("trans_amt_p", trans_amt);
                trans_amt_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(trans_amt_p);

                OracleParameter crncy_cde_p = new OracleParameter("crncy_cde_p", crncy_cde);
                crncy_cde_p.Direction = ParameterDirection.Input;
                //sms_date_p.OracleType = OracleType.Number;
                cmd.Parameters.Add(crncy_cde_p);

                OracleParameter merc_name_p = new OracleParameter("merc_name_p", merc_name);
                merc_name_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(merc_name_p);

                OracleParameter approve_code_p = new OracleParameter("approve_code_p", approve_code);
                approve_code_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(approve_code_p);

                OracleParameter trans_type_p = new OracleParameter("trans_type_p", trans_type);
                trans_type_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(trans_type_p);

                OracleParameter update_datetime_p = new OracleParameter("update_datetime_p", update_datetime);
                update_datetime_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(update_datetime_p);

                OracleParameter sms_stat_p = new OracleParameter("sms_stat_p", sms_stat);
                sms_stat_p.Direction = ParameterDirection.Input;
                cmd.Parameters.Add(sms_stat_p);

                OracleString rowID;
                int insertedRow = 0;

                
                insertedRow = cmd.ExecuteOracleNonQuery(out rowID);
                if (flag == true)
                    CloseConnection();
                return insertedRow;
            }
            catch (Exception ex)
            {
                classDWFPTLogWriter.WriteLog("Error InsertGD_SMS_ToDW()at DataAccess, " + ex.Message);
                if (flag == true)
                    CloseConnection();
                return 0;
            }
            //CloseConnection();// dong ket noi CSDL   

        }

    }
}
