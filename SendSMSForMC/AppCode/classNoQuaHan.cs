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
    class classNoQuaHan
    {
        public static bool _exitThread = false;
        public static string _updateDataTime = null;
        private static classDataAccess _dataAccess = new classDataAccess();
        public static string SMS_TYPE ="TNNQH";
        public static string SMS_TYPE_IPP = "NQHIPP";
        private static string SCBPhone="";

        public static void RunService()
        {
            int hour = 0;
            int minute = 0;
            //int day_p;

            int hourValue = 0;
            int minuteValue = 0;

            DataTable table = new DataTable();
            DataTable table_IPP = new DataTable();

            SCBPhone = classUtilities.GetStringValueFromConfig("SCB_Contact_Phone");
            hourValue = classUtilities.GetIntValueFromConfig("Accumulative_NoQuaHan_Hour");
            minuteValue = classUtilities.GetIntValueFromConfig("Accumulative_NoQuaHan_Minute");

           while (_exitThread == false)
            {
                try
                {
                    hour = DateTime.Now.Hour;
                    minute = DateTime.Now.Minute;
                    DateTime day_p = DateTime.Today;

                    if (hour == hourValue && minute == minuteValue)                  
                    //if(1==1)//hhhh
                    {

                            classNoQuaHanLogWriter.WriteLog("-------------------Begin Process----------------- at: " + DateTime.Now.ToString());
                            _dataAccess = new classDataAccess();
                            //table_IPP.Rows.Clear();
                            //table_IPP = Get_NoQuaHan_IPP();
                            //if (table_IPP.Rows.Count > 0)
                            //{
                            //    Insert_SMSMessage_Email_IPP(table_IPP);

                            //}
                        
                            table.Rows.Clear();                            
                            table = Get_NoQuaHan();
                            if (table.Rows.Count > 0)
                            {
                                Insert_SMSMessage_Email(table);

                            }
                            classNoQuaHanLogWriter.WriteLog("----------------End Process----------------- at: " + DateTime.Now.ToString());
                            
                            Thread.Sleep(1000 * 60);// de khong lap lap lai vong lap lan 2 trong ngay

                        }
                        else
                        {
                            if ((hour == hourValue && minute > minuteValue) || (hour > hourValue))// da qua gio thuc thi, sleep de chuyen den ngay tiep theo
                            {
                                classNoQuaHanLogWriter.WriteLog("((hour == hourValue && minute > minuteValue) || (hour > hourValue)) sleep(minute): " + (60 * (24 - (hour - hourValue + 1))));
                                Thread.Sleep(1000 * (60 * 60) * (24 - (hour - hourValue + 1)));

                            }
                            else
                            {
                                if (hour < (hourValue - 1))
                                {
                                    classNoQuaHanLogWriter.WriteLog("(hour < hourValue) sleep(miute): " + ((hourValue - hour - 1) * 60 - minute));
                                    Thread.Sleep(1000 * 60 * ((hourValue - hour - 1) * 60 - minute + minuteValue - 1));
                                }
                                else
                                {
                                    if (hour == (hourValue - 1))
                                    {
                                        classNoQuaHanLogWriter.WriteLog("(hour == (hourValue - 1)) sleep(minute): " + (60 - minute + minuteValue - 1));
                                        Thread.Sleep((1000 * 60) * (60 - minute + minuteValue - 1));
                                        if ((60 - minute + minuteValue - 1) == 0)//1 phut truoc gio gui tn
                                            Thread.Sleep(1000 * 10);
                                    }
                                    else//(hour==hourValue)
                                    {
                                        if (minute < (minuteValue - 1))
                                        {
                                            classNoQuaHanLogWriter.WriteLog("(minute <= (minuteValue - 1)) sleep(minute): " + (minuteValue - minute - 1));
                                            Thread.Sleep((1000 * 60) * (minuteValue - minute - 1));
                                        }
                                        else
                                        {
                                            classNoQuaHanLogWriter.WriteLog("sleep(ss): " + 10);
                                            Thread.Sleep(1000 * 10);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    

              
                catch (Exception ex)
                {
                    classNoQuaHanLogWriter.WriteLog("Error RunService(), " + ex.Message);
                }
            }

        }
        public static DataTable Get_NoQuaHan_IPP()
        {
            try
            {
                string month = DateTime.Now.AddMonths(-1).ToString("yyyyMM");
                DataTable table = new DataTable();
                string maxUpdateDT = null;          
                if (string.IsNullOrEmpty(_updateDataTime) == false)
                {
                    maxUpdateDT = _updateDataTime;
                    _updateDataTime = null;
                }
                else
                {

                    maxUpdateDT = _dataAccess.Get_Max_UpdateTime(SMS_TYPE_IPP);
                   
                }
                if (string.IsNullOrEmpty(maxUpdateDT) == false)
                {
                    try
                    {
                        long MaxUpdateTime = long.Parse(maxUpdateDT);
                    }
                    catch (Exception ex)
                    {
                        classNoQuaHanLogWriter.WriteLog("Error Get_NoQuaHan_IPP(), " + ex.Message);
                        return null;
                    }
                    if (maxUpdateDT != null)
                        table = _dataAccess.GetNoQuaHan_IPP(month, maxUpdateDT);
                }
                return table;
            }
            catch (Exception e)
            {
                classNoQuaHanLogWriter.WriteLog("Err Get_NoQuaHan_IPP(): " + e.ToString());
                return null;
            }
        }

        public static DataTable Get_NoQuaHan()
        {
            try
            {

                string maxUpdateDT = null;
                DataTable table = new DataTable();
                if (string.IsNullOrEmpty(_updateDataTime) == false)
                {
                    maxUpdateDT = _updateDataTime;
                    _updateDataTime = null;
                }
                else
                {

                    maxUpdateDT = _dataAccess.Get_Max_UpdateTime(SMS_TYPE);
                   
                }
                if (string.IsNullOrEmpty(maxUpdateDT) == false)
                {
                    try
                    {
                        long MaxUpdateTime = long.Parse(maxUpdateDT);
                    }
                    catch (Exception ex)
                    {
                        classNoQuaHanLogWriter.WriteLog("Error Get_NoQuaHan(), " + ex.Message);
                        return null;
                    }
                    if (maxUpdateDT != null)
                        table = _dataAccess.GetNoQuaHan(maxUpdateDT);
                }
                return table;
            }
            catch (Exception e)
            {
                classNoQuaHanLogWriter.WriteLog("Err Get_NoQuaHan(): " + e.ToString());
                return null;
            }
        }
        private static void Insert_SMSMessage_Email_IPP(DataTable table)
        {
            try
            {
                classDataAccess ebankDataAccess = new classDataAccess();
                classDataAccess dwDataAccess = new classDataAccess();
                ebankDataAccess.OpenConnection("EBANK_GW");
                dwDataAccess.OpenConnection("CW_DW");

                string message = "";
                int result = 0;
                int count = 0;
                int count2 = 0;
                int count_err = 0;
                foreach (DataRow row in table.Rows)
                {
                    string due_date = row.ItemArray[6].ToString();
                    DateTime dua_date_p = new DateTime(int.Parse(due_date.Substring(0, 4)), int.Parse(due_date.Substring(4, 2)), int.Parse(due_date.Substring(6, 2)));

                    if (System.DateTime.Today == dua_date_p.AddDays(classUtilities.GetIntValueFromConfig("date1_NoQuaHan")) ||
                        System.DateTime.Today == dua_date_p.AddDays(classUtilities.GetIntValueFromConfig("date2_NoQuaHan")) ||
                        System.DateTime.Today == dua_date_p.AddDays(classUtilities.GetIntValueFromConfig("date3_NoQuaHan"))
                       )
                    //if (1 == 1) //hhhh
                    {

                        result = 0;

                        //message = CreateSMSMessage(row.ItemArray[1].ToString(), row.ItemArray[0].ToString(), row.ItemArray[10].ToString(), row.ItemArray[8].ToString(), row.ItemArray[11].ToString(), row.ItemArray[14].ToString(), row.ItemArray[15].ToString());
                        message = CreateSMSMessage_new_IPP(row.ItemArray[1].ToString(), row.ItemArray[0].ToString(), DateTime.Parse(row.ItemArray[10].ToString()), row.ItemArray[8].ToString(), row.ItemArray[11].ToString(), row.ItemArray[14].ToString(), row.ItemArray[15].ToString());

                        if (string.IsNullOrEmpty(message) == false)
                        {


                            //string mobile = classUtilities.GetMobileFromCardNoOfSpecialList(row.ItemArray[0].ToString(), row.ItemArray[3].ToString());
                            string mobile = row.ItemArray[3].ToString();
                            if (row.ItemArray[3].ToString() == "khong co")
                            {
                                result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                                row.ItemArray[12].ToString()
                                                                               , mobile
                                                                               , message
                                                                               , 'Y'//Y se ko gui tin nhan, N se gui tin nhan
                                                                               , SMS_TYPE_IPP);
                            }
                            else
                            {
                                //result = 1;
                                result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                                    row.ItemArray[12].ToString()
                                                                                   , mobile
                                                                                   , message
                                                                                   , 'N'//Y se ko gui tin nhan, N se gui tin nhan
                                                                                   , SMS_TYPE_IPP);
                            }
                            if (result == 1)
                            {
                                int temp = dwDataAccess.Insert_MASTERCARD_EMAIL
                                    (
                                      row.ItemArray[4].ToString()
                                    , row.ItemArray[2].ToString()
                                    , double.Parse(row.ItemArray[5].ToString())
                                    , double.Parse(row.ItemArray[11].ToString())
                                    , double.Parse(row.ItemArray[8].ToString())
                                    , row.ItemArray[1].ToString()
                                    );

                                if (temp == 1)
                                {
                                    int temp2 = 0;
                                    if (row.ItemArray[3].ToString() == "khong co")
                                    {
                                        temp2 = dwDataAccess.InsertNoQuaHanSMSToDW(
                                            SMS_TYPE_IPP
                                        , message
                                        , mobile
                                        , DateTime.Parse(row.ItemArray[10].ToString())
                                            //, row.ItemArray[0].ToString()
                                        , row.ItemArray[13].ToString()
                                        , row.ItemArray[1].ToString()
                                        , row.ItemArray[9].ToString()
                                        , "Y"
                                        );
                                    }
                                    else
                                    {
                                        temp2 = dwDataAccess.InsertNoQuaHanSMSToDW(
                                            SMS_TYPE_IPP
                                        , message
                                        , mobile
                                        , DateTime.Parse(row.ItemArray[10].ToString())
                                            //, row.ItemArray[0].ToString()
                                        , row.ItemArray[13].ToString()
                                        , row.ItemArray[1].ToString()
                                        , row.ItemArray[9].ToString()
                                        , "N"
                                        );
                                    }

                                    count++;
                                    if (temp2 == 1)
                                        count2++;
                                }
                            }
                            else
                            {
                                count_err = dwDataAccess.InsertNoQuaHanSMSToDW(
                                            SMS_TYPE_IPP
                                        , message
                                        , mobile
                                        , DateTime.Parse(row.ItemArray[10].ToString())
                                    //, row.ItemArray[0].ToString()
                                        , row.ItemArray[13].ToString()
                                        , row.ItemArray[1].ToString()
                                        , row.ItemArray[9].ToString()
                                        , "E"
                                        );
                            }

                        }
                    }
                }
                ebankDataAccess.CloseConnection();
                dwDataAccess.CloseConnection();
                classNoQuaHanLogWriter.WriteLog("Message da duoc Insert vao EbankGW thanh cong: " + count2);
                classNoQuaHanLogWriter.WriteLog("Message loi khong Insert vao EbankGW: " + count_err);
                classNoQuaHanLogWriter.WriteLog("Message da duoc Insert vao XMIS thanh cong: " + count);
            }
            catch (Exception e)
            {
                classNoQuaHanLogWriter.WriteLog("Err Insert_SMSMessage_Email_IPP(): " + e.ToString());
            }

        }

        private static void Insert_SMSMessage_Email(DataTable table)
        {
            try
            {
                classDataAccess ebankDataAccess = new classDataAccess();
                classDataAccess dwDataAccess = new classDataAccess();
                ebankDataAccess.OpenConnection("EBANK_GW");
                dwDataAccess.OpenConnection("CW_DW");

                string message = "";
                int result = 0;
                int count = 0;
                int count2 = 0;
                int count_err = 0;
                foreach (DataRow row in table.Rows)
                {
                    string due_date = row.ItemArray[6].ToString();
                    DateTime dua_date_p = new DateTime(int.Parse(due_date.Substring(0, 4)), int.Parse(due_date.Substring(4, 2)), int.Parse(due_date.Substring(6, 2)));

                    if (System.DateTime.Today == dua_date_p.AddDays(classUtilities.GetIntValueFromConfig("date1_NoQuaHan")) ||
                        System.DateTime.Today == dua_date_p.AddDays(classUtilities.GetIntValueFromConfig("date2_NoQuaHan")) ||
                        System.DateTime.Today == dua_date_p.AddDays(classUtilities.GetIntValueFromConfig("date3_NoQuaHan"))
                       )
                    //if (1 == 1) //hhhh
                    {

                        result = 0;
                        
                        //message = CreateSMSMessage(row.ItemArray[1].ToString(), row.ItemArray[0].ToString(), row.ItemArray[10].ToString(), row.ItemArray[8].ToString(), row.ItemArray[11].ToString(), row.ItemArray[14].ToString(), row.ItemArray[15].ToString());
                        message = CreateSMSMessage_new(row.ItemArray[1].ToString(), row.ItemArray[0].ToString(),DateTime.Parse(row.ItemArray[10].ToString()), row.ItemArray[8].ToString(), row.ItemArray[11].ToString(), row.ItemArray[14].ToString(), row.ItemArray[15].ToString());

                        if (string.IsNullOrEmpty(message) == false)
                        {


                            string mobile = classUtilities.GetMobileFromCardNoOfSpecialList(row.ItemArray[0].ToString(), row.ItemArray[3].ToString());
                            if (row.ItemArray[3].ToString() == "khong co")
                            {
                                result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                                row.ItemArray[12].ToString()
                                                                               , mobile
                                                                               , message
                                                                               , 'Y'//Y se ko gui tin nhan, N se gui tin nhan
                                                                               , SMS_TYPE);
                            }
                            else
                            {
                                //result = 1;
                                result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                                    row.ItemArray[12].ToString()
                                                                                   , mobile
                                                                                   , message
                                                                                   , 'N'//Y se ko gui tin nhan, N se gui tin nhan
                                                                                   , SMS_TYPE);
                            }
                            if (result == 1)
                            {
                                int temp = dwDataAccess.Insert_MASTERCARD_EMAIL
                                    (
                                      row.ItemArray[4].ToString()
                                    , row.ItemArray[2].ToString()
                                    , double.Parse(row.ItemArray[5].ToString())
                                    , double.Parse(row.ItemArray[11].ToString())
                                    , double.Parse(row.ItemArray[8].ToString())
                                    , row.ItemArray[1].ToString()
                                    );

                                if (temp == 1)
                                {
                                    int temp2 = 0;
                                    if (row.ItemArray[3].ToString() == "khong co")
                                    {
                                        temp2 = dwDataAccess.InsertNoQuaHanSMSToDW(
                                            SMS_TYPE
                                        , message
                                        , mobile
                                        , DateTime.Parse(row.ItemArray[10].ToString())
                                            //, row.ItemArray[0].ToString()
                                        , row.ItemArray[13].ToString()
                                        , row.ItemArray[1].ToString()
                                        , row.ItemArray[9].ToString()
                                        , "Y"
                                        );
                                    }
                                    else
                                    {
                                        temp2 = dwDataAccess.InsertNoQuaHanSMSToDW(
                                            SMS_TYPE
                                        , message
                                        , mobile
                                        , DateTime.Parse(row.ItemArray[10].ToString())
                                            //, row.ItemArray[0].ToString()
                                        , row.ItemArray[13].ToString()
                                        , row.ItemArray[1].ToString()
                                        , row.ItemArray[9].ToString()
                                        , "N"
                                        );
                                    }

                                    count++;
                                    if (temp2 == 1)
                                        count2++;
                                }
                            }
                            else
                            {
                                count_err = dwDataAccess.InsertNoQuaHanSMSToDW(
                                            SMS_TYPE
                                        , message
                                        , mobile
                                        , DateTime.Parse(row.ItemArray[10].ToString())
                                    //, row.ItemArray[0].ToString()
                                        , row.ItemArray[13].ToString()
                                        , row.ItemArray[1].ToString()
                                        , row.ItemArray[9].ToString()
                                        , "E"
                                        );
                            }
                           
                        }
                    }
                }
                ebankDataAccess.CloseConnection();
                dwDataAccess.CloseConnection();
                classNoQuaHanLogWriter.WriteLog("Message da duoc Insert vao EbankGW thanh cong: " + count2);
                classNoQuaHanLogWriter.WriteLog("Message loi khong Insert vao EbankGW: " + count_err);
                classNoQuaHanLogWriter.WriteLog("Message da duoc Insert vao XMIS thanh cong: " + count);
            }
            catch (Exception e)
            {
                classNoQuaHanLogWriter.WriteLog("Err Insert_SMSMessage_Email(): " + e.ToString());
            }
            
        }
        private static string CreateSMSMessage_new_IPP(string brand, string pan, DateTime date, string ngay_ctt, string so_tien_tttt, string vip_card, string vip_cif)
        {
            try
            {
                if (vip_card == "Y" || vip_cif == "Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                double amt = double.Parse(so_tien_tttt);
                string amount1 = string.Format("{0:#,##0.##}", amt);
                pan = pan.Substring(12, 4);
                //string date_t = date.Substring(0, 2) + "/" + date.Substring(3, 2) + "/" + date.Substring(8, 2);
                string date_t = date.AddDays(-1).ToShortDateString();
                string tmp_smsMessage = "Tinh den " + date_t + ", the " + brand + " x" + pan + " da cham thanh toan tra gop " + ngay_ctt
                + " ngay.\nVui long thanh toan toi thieu " + amount1 + "VND de duy tri lich su tin dung tot.\nLH " + SCBPhone;
                return tmp_smsMessage;
            }
            catch (Exception ex)
            {
                classNoQuaHanLogWriter.WriteLog("Error CreateSMSMessage_new_IPP(), " + ex.Message);
                return "";
            }
        }
        private static string CreateSMSMessage_new(string brand, string pan, DateTime date, string ngay_ctt, string so_tien_tttt, string vip_card, string vip_cif)
        {
            try
            {
                if (vip_card == "Y" || vip_cif == "Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                double amt = double.Parse(so_tien_tttt);
                string amount1 = string.Format("{0:#,##0.##}", amt);
                pan = pan.Substring(12,4);
                //string date_t = date.Substring(0, 2) + "/" + date.Substring(3, 2) + "/" + date.Substring(8, 2);
                string date_t = date.AddDays(-1).ToShortDateString();               
                string tmp_smsMessage = "Tinh den " + date_t + ", the " + brand + " x" + pan + " da cham thanh toan " + ngay_ctt
                + " ngay. Vui long thanh toan toi thieu " + amount1 + "VND de duy tri lich su tin dung tot. LH " + SCBPhone;
                return tmp_smsMessage;
            }
            catch (Exception ex)
            {
                classNoQuaHanLogWriter.WriteLog("Error CreateSMSMessage(), " + ex.Message);
                return "";
            }
        }

        private static string CreateSMSMessage(string brand, string pan, string date, string ngay_ctt, string so_tien_tttt, string vip_card, string vip_cif)
        {
            try
            {
                if (vip_card == "Y" || vip_cif == "Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                double amt = double.Parse(so_tien_tttt);
                string amount1 = string.Format("{0:#,##0.##}", amt);
                pan = pan.Substring(12, 4);           
                string date_t = date.Substring(0, 2) + "/" + date.Substring(3, 2) + "/" + date.Substring(8, 2);
               
                string tmp_smsMessage = "Tinh den " + date_t + ", the SCB" + brand + " x" + pan + " da cham thanh toan " + ngay_ctt
                + " ngay\nVui long thanh toan toi thieu " + amount1 + "VND de duy tri lich su tin dung tot\nChi tiet " + SCBPhone;
                return tmp_smsMessage;
            }
            catch (Exception ex)
            {
                classNoQuaHanLogWriter.WriteLog("Error CreateSMSMessage(), " + ex.Message);
                return "";
            }            
        }
    }
}
