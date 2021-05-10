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
    class classAnnual_Fee
    {
        public static bool _exitThread = false;
        public static string _updateDateTime = null;

        private static classDataAccess _dataAccess = new classDataAccess();

        public static string SMS_TYPE = "GDPHI";

        private static string SCBPhone = "";

        public static void RunService()
        {

            int minute = 0;
            int value = 0;
            DataTable table = new DataTable();
            SCBPhone = classUtilities.GetStringValueFromConfig("SCB_Contact_Phone");
            value = classUtilities.GetIntValueFromConfig("Annual_Fee_Minute");
            string begin_h = classUtilities.GetStringValueFromConfig("Be_AnnuaFee");
            string end_h = classUtilities.GetStringValueFromConfig("En_AnnuaFee");

            while (_exitThread == false)
            {
                try
                {
                    minute = DateTime.Now.Minute;
                    if (int.Parse(System.DateTime.Now.Hour.ToString()) >= int.Parse(begin_h) && int.Parse(System.DateTime.Now.Hour.ToString()) <= int.Parse(end_h))
                    //if(1==1)//hhhh
                    {

                        if (minute % value == 0)
                        //if(1==1)//hhhh
                        {


                            classAnnual_FeeLogWriter.WriteLog("----------------Begin Process-----------------");
                            _dataAccess = new classDataAccess();
                            table.Rows.Clear();
                            table = Get_Annual_Fee();
                            if (table.Rows.Count > 0)
                                Insert_SMSMessage(table);

                            classAnnual_FeeLogWriter.WriteLog("----------------End Process-----------------");
                           
                        }
                        if (value > 2)
                        {
                            if ((value - (minute % value) - 1) > 0)
                            {
                                classAnnual_FeeLogWriter.WriteLog("sleep " + (value - (minute % value) - 1) + " minute");
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
                }
                catch (Exception ex)
                {
                    classAnnual_FeeLogWriter.WriteLog("Error RunService(), " + ex.Message);
                }
            }
        }

        private static DataTable Get_Annual_Fee()
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
                    classAnnual_FeeLogWriter.WriteLog("Error Get_Annual_Fee(), " + ex.Message);
                    return null;
                }
                if (maxUpdateDT != null)
                    table = _dataAccess.GetAnnualFee(maxUpdateDT);
            }
            return table;
        }


        private static void Insert_SMSMessage(DataTable table)
        {
            classDataAccess ebankDataAccess = new classDataAccess();
            classDataAccess dwDataAccess = new classDataAccess();
            ebankDataAccess.OpenConnection("EBANK_GW");
            dwDataAccess.OpenConnection("CW_DW");

            string message = "";
            int result = 0;
            int count = 0;
            int count_err = 0;

            //OracleCommand cmd = ebankDataAccess.AddProcedureParameterToEBankGW();           l

            foreach (DataRow row in table.Rows)
            {
                result = 0;

                message = CreateSMSMessage(row.ItemArray[5].ToString(), row.ItemArray[1].ToString(), row.ItemArray[3].ToString(),
                                                        row.ItemArray[2].ToString(), row.ItemArray[4].ToString(), row.ItemArray[11].ToString(), row.ItemArray[12].ToString());
                if (string.IsNullOrEmpty(message) == false)
                {
                    //string mobile = classUtilities.GetMobileFromCardNoOfSpecialList(row.ItemArray[10].ToString(), row.ItemArray[7].ToString());
                    string count_act = _dataAccess.Get_First_Activate(row.ItemArray[10].ToString());
                    string count_rep = _dataAccess.Get_Reply_Card(row.ItemArray[10].ToString());
                    if (count_act == "1" && count_rep == "0" && row.ItemArray[13].ToString() == row.ItemArray[3].ToString())//count_rep=0: the chua thay the, count_act=1: active lan dau, ngay active=ngay thu phi
                    {
                        classAnnual_FeeLogWriter.WriteLog("khong gui sms cho lan thu phi dau: card=" + row.ItemArray[10].ToString());
                    }
                    else
                    {

                        if (row.ItemArray[7].ToString() == "khong co")
                        {
                            result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                               row.ItemArray[9].ToString()
                                //, mobile
                                                                               , row.ItemArray[7].ToString()
                                                                               , message
                                                                               , 'Y'//Y se ko gui tin nhan, N se gui tin nhan
                                                                               , SMS_TYPE);
                        }
                        else
                        {
                            result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                                row.ItemArray[9].ToString()
                                //, mobile
                                                                                , row.ItemArray[7].ToString()
                                                                                , message
                                                                                , 'N'//Y se ko gui tin nhan, N se gui tin nhan
                                                                                , SMS_TYPE);
                        }
                        if (result == 1)
                        {

                            if (row.ItemArray[10].ToString() == "khong co")
                            {
                                count += dwDataAccess.InsertAnnualFeeSMSToDW(
                                                                    SMS_TYPE
                                                                    , message
                                    //, mobile//    ,long.Parse(row.ItemArray[10].ToString())
                                                                    , row.ItemArray[7].ToString()
                                                                    , DateTime.Parse(row.ItemArray[0].ToString())
                                    //, row.ItemArray[4].ToString()
                                                                    , row.ItemArray[10].ToString()
                                                                    , row.ItemArray[5].ToString()
                                                                    , row.ItemArray[6].ToString()
                                                                    , row.ItemArray[3].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                                    , row.ItemArray[2].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                                    , double.Parse(row.ItemArray[4].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                                                    , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())                                                               
                                                                    , "Y"
                                                                    );
                            }
                            else
                            {
                                count += dwDataAccess.InsertAnnualFeeSMSToDW(
                                                                    SMS_TYPE
                                                                    , message
                                    //, mobile//    ,long.Parse(row.ItemArray[10].ToString())
                                                                    , row.ItemArray[7].ToString()
                                                                    , DateTime.Parse(row.ItemArray[0].ToString())
                                    //, row.ItemArray[4].ToString()
                                                                    , row.ItemArray[10].ToString()
                                                                    , row.ItemArray[5].ToString()
                                                                    , row.ItemArray[6].ToString()
                                                                    , row.ItemArray[3].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                                    , row.ItemArray[2].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                                    , double.Parse(row.ItemArray[4].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                                                    , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())    
                                                                    , "N"
                                                                    );
                            }
                        }
                        else
                        {
                            count_err += dwDataAccess.InsertAnnualFeeSMSToDW(
                                                                   SMS_TYPE
                                                                   , message
                                //, mobile//    ,long.Parse(row.ItemArray[10].ToString())
                                                                   , row.ItemArray[7].ToString()
                                                                   , DateTime.Parse(row.ItemArray[0].ToString())
                                //, row.ItemArray[4].ToString()
                                                                   , row.ItemArray[10].ToString()
                                                                    , row.ItemArray[5].ToString()
                                                                    , row.ItemArray[6].ToString()
                                                                    , row.ItemArray[3].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                                    , row.ItemArray[2].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                                    , double.Parse(row.ItemArray[4].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                                                    , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())    
                                                                   , "E"
                                                                   );
                        }
                    }
                }

            }
            ebankDataAccess.CloseConnection();
            dwDataAccess.CloseConnection();
            classAnnual_FeeLogWriter.WriteLog("Message da duoc Insert vao EbankGW thanh cong: " + count);
            classAnnual_FeeLogWriter.WriteLog("Message loi khong Insert vao EbankGW: " + count_err);
            return;
        }


        private static string CreateSMSMessage(string brand, string pan, string date, string time,
                                       string amount, string vip_card, string vip_cif)
        {
            try
            {
                pan = pan.Substring(12, 4);
                date = date.Substring(6, 2) + "/" + date.Substring(4, 2) + "/" + date.Substring(2, 2);
                time = time.Substring(0, 2) + ":" + time.Substring(2, 2);
                double amt = double.Parse(amount);
                string amount1 = string.Format("{0:#,##0.##}", amt);
                if (vip_card == "Y" || vip_cif =="Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";

                string smsMessage = "SCB thu phi thuong nien "+ amount + " VND the " + brand + " X" + pan + " " 
                + "luc " + time + ", ngay " + date + ". Chi tiet vui long lien he Hotline SCB(24/7): "+SCBPhone+". Cam on Quy khach";

                return smsMessage;
            }
            catch (Exception ex)
            {
                classAnnual_FeeLogWriter.WriteLog("Error CreateSMSMessage(), " + ex.Message);
                return "";
            }
        }
    }
}
