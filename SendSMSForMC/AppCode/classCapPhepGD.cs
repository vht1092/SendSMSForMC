using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading;
using System.Windows.Forms;
using System.Data;
using SendSMSForMC.AppCode;
namespace SendSMSForMC.AppCode
{
    class classCapPhepGD
    {
        public static bool _exitThread = false;
        public static string _updateDateTime = null;

        private static classDataAccess _dataAccess = new classDataAccess();

        public static string SMS_TYPE = "CPGD";

        private static string SCBPhone = "";

        public static void RunService()
        {

            int minute = 0;
            int value = 0;
            DataTable table = new DataTable();
            SCBPhone = classUtilities.GetStringValueFromConfig("SCB_Contact_Phone");
            value = classUtilities.GetIntValueFromConfig("CPGD_Minute");
            while (_exitThread == false)
            {
                try
                {
                    minute = DateTime.Now.Minute;

                    if (minute % value == 0)                    {

                        classCapPhepGDLogWriter.WriteLog("----------------Begin Process-----------------");
                        _dataAccess = new classDataAccess();
                        table.Rows.Clear();
                        table = Get_CapPhepGD();
                        if (table.Rows.Count > 0)
                            Insert_SMSMessage(table);

                        classCapPhepGDLogWriter.WriteLog("----------------End Process-----------------");
                        
                    }
                    if (value > 2)
                    {
                        if ((value - (minute % value) - 1) > 0)
                        {
                            classCapPhepGDLogWriter.WriteLog("sleep " + (value - (minute % value) - 1) + " minute");
                            Thread.Sleep(1000 * (value - (minute % value) - 1) * 55);
                        }
                        else
                        {
                            Thread.Sleep(1000 * 10);// truong hop start vao -1 ph
                        }
                    }
                    else
                        Thread.Sleep(1000 * 55);
                }
                catch (Exception ex)
                {
                    classCapPhepGDLogWriter.WriteLog("Error RunService(), " + ex.Message);
                }
            }
        }

        private static DataTable Get_CapPhepGD()
        {
            string maxUpdateDT = null;
            DataTable table = new DataTable();
            if (string.IsNullOrEmpty(_updateDateTime) == false)
            {
                maxUpdateDT = _updateDateTime;
                _updateDateTime = null;
            }
            else
            {
                maxUpdateDT = _dataAccess.Get_Max_UpdateTime(SMS_TYPE);
               
            }
            if (string.IsNullOrEmpty(maxUpdateDT) == false)
            {
                try
                {
                    long maxUpdateTime = long.Parse(maxUpdateDT);
                }
                catch (Exception ex)
                {
                    classCapPhepGDLogWriter.WriteLog("Error Get_CapPhepGD(), " + ex.Message);
                    return null;
                }
                if (maxUpdateDT != null)
                    table = _dataAccess.GetCapPhepGD(maxUpdateDT);
            }
            return table;
        }


        private static void Insert_SMSMessage(DataTable table)
        {
            classDataAccess ebankDataAccess = new classDataAccess();
            classDataAccess dwDataAccess = new classDataAccess();
            ebankDataAccess.OpenConnection("EBANK_GW");//hhhh
            dwDataAccess.OpenConnection("CW_DW");

            string message = "";
            int result = 0;
            int succ_eb = 0;
            int err_eb = 0;
            int succ_dw = 0;
            int err_dw = 0;

            //OracleCommand cmd = ebankDataAccess.AddProcedureParameterToEBankGW();           l

            foreach (DataRow row in table.Rows)
            {
                result = 0;
                message = CreateSMSMessage(row.ItemArray[2].ToString(), row.ItemArray[3].ToString(), row.ItemArray[4].ToString(), row.ItemArray[5].ToString(),
                    row.ItemArray[6].ToString(), row.ItemArray[10].ToString(), row.ItemArray[11].ToString(), row.ItemArray[12].ToString(), row.ItemArray[13].ToString());
               
                if (string.IsNullOrEmpty(message) == false)
                {
                    
                    
                        //string mobile = classUtilities.GetMobileFromCardNoOfSpecialList(row.ItemArray[15].ToString(), row.ItemArray[10].ToString());
                        if (row.ItemArray[10].ToString() == "khong co")                        
                        {
                            int flag_fpt = dwDataAccess.InsertGD_SMS_ToDW(SMS_TYPE, message, row.ItemArray[7].ToString(),
                                DateTime.Parse(row.ItemArray[0].ToString()), row.ItemArray[9].ToString()
                                , row.ItemArray[2].ToString(), "", row.ItemArray[4].ToString(), row.ItemArray[5].ToString()
                                , double.Parse(row.ItemArray[6].ToString()), "", "", "", "", row.ItemArray[1].ToString(), "Y");

                            if (flag_fpt == 0)
                            {
                                classCapPhepGDLogWriter.WriteLog("err: card no " + row.ItemArray[9].ToString() + " can't insert DB DW");
                                err_dw++;

                            }
                            else
                            {
                                result = ebankDataAccess.InsertSMSMessateToEBankGW_2(row.ItemArray[8].ToString(), row.ItemArray[7].ToString(),message,
                                    'Y'
                                    , SMS_TYPE);

                               
                                if (result == 0)
                                {
                                    err_eb++;
                                    classCapPhepGDLogWriter.WriteLog("err: card no " + row.ItemArray[9].ToString() + " can't insert DB EB");
                                }
                                else
                                    succ_eb++;
                            }
                        }
                        else // so phone hop le
                        {
                            int flag_fpt = dwDataAccess.InsertGD_SMS_ToDW(SMS_TYPE, message, row.ItemArray[7].ToString(),
                                DateTime.Parse(row.ItemArray[0].ToString()), row.ItemArray[9].ToString()
                                , row.ItemArray[2].ToString(), "", row.ItemArray[4].ToString(), row.ItemArray[5].ToString()
                                , double.Parse(row.ItemArray[6].ToString()), "", "", "", "", row.ItemArray[1].ToString(), "N");
                            if (flag_fpt == 0)
                            {
                                classCapPhepGDLogWriter.WriteLog("err: card no " + row.ItemArray[9].ToString() + " can't insert DB DW");
                                err_dw++;

                            }
                            else
                            {
                                result = ebankDataAccess.InsertSMSMessateToEBankGW_2(row.ItemArray[8].ToString(), row.ItemArray[7].ToString(), message,
                                    'N'
                                    , SMS_TYPE);
                                if (result == 0)
                                {
                                    dwDataAccess.Update_Status_SMS_CPGD(//update status sms ve loi ko gui qua EW
                                            row.ItemArray[9].ToString(),
                                            row.ItemArray[4].ToString(),
                                            row.ItemArray[1].ToString(),
                                            "E"
                                            );
                                    classCapPhepGDLogWriter.WriteLog("err: card no " + row.ItemArray[9].ToString() + " can't insert DB EB");
                                    err_eb++;
                                }
                                else
                                    succ_eb++;
                            }
                        }
                    
                  
                }

            }
            ebankDataAccess.CloseConnection();
            dwDataAccess.CloseConnection();
            classCapPhepGDLogWriter.WriteLog("Message da duoc Insert vao EbankGW thanh cong: " + succ_eb);
            classCapPhepGDLogWriter.WriteLog("Message loi khong Insert vao EbankGW: " + err_eb);
            classCapPhepGDLogWriter.WriteLog("Message da duoc Insert vao DW thanh cong: " + succ_dw);
            classCapPhepGDLogWriter.WriteLog("Message loi khong Insert vao DW: " + err_dw);
            return;
        }


        private static string CreateSMSMessage(string brand, string pan, string date, string time,
        string amount,string card_aval, string MSL_aval,string vip_card, string vip_cif)
        {
            try
            {
                string pan_format = pan.Substring(12, 4);
                string date_format = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);               
                double amt = double.Parse(amount);
                string amt_format = string.Format("{0:#,##0.##}", amt);
                string han_muc_format=null;
                double card_aval_p = double.Parse(card_aval);
                double MSL_aval_p = double.Parse(MSL_aval);
                if (card_aval_p > MSL_aval_p)
                    han_muc_format = string.Format("{0:#,##0.##}", MSL_aval_p);
                else
                    han_muc_format = string.Format("{0:#,##0.##}", card_aval_p);           
                string time_format = time.Substring(0, 2) + ":" + time.Substring(2, 2);
                string hot_line = "";
                if (vip_card == "Y" || vip_cif =="Y")
                    hot_line = "1800545438";
                else
                    hot_line = "19006538";
                string smsMessage = "SCB thu phi cap phep giao dich the  " + brand + " x" + pan.Substring(12,4)
                + ": " + amt_format + "VND luc " + time_format + " ngay " + date_format + ". HM con lai " + han_muc_format + "VND. Chi tiet lien he " + hot_line;
                return smsMessage;
            }
            catch (Exception ex)
            {
                classCapPhepGDLogWriter.WriteLog("Error CreateSMSMessage(), " + ex.Message);
                return "";
            }
        }
    }
}
