using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Windows.Forms;
using System.IO;
using SendSMSForMC.AppCode;
using System.Data;
using System.Data.OracleClient;

namespace SendSMSForMC.AppCode
{
    class classThuNoFail
    {
        public static bool exitThread = false;
        public static string _updateDateTime = null;

        private static classDataAccess _dataAccess = new classDataAccess();

        public static string SMS_TYPE = "THUNO";

        private static string SCBPhone = "";

        public static void RunService()
        {
            int hour = 0;
            int minute = 0;
            int dayValue1 = 0;
            int hourValue = 0;
            int minuteValue = 0;
            DataTable table = new DataTable();
            DataTable table_duadate = new DataTable();
            DataTable table_cre_card = new DataTable();

            SCBPhone = classUtilities.GetStringValueFromConfig("SCB_Contact_Phone");
            hourValue = classUtilities.GetIntValueFromConfig("ThuNoFail_Hour");
            minuteValue = classUtilities.GetIntValueFromConfig("ThuNoFail_Minute");
            string mobile = classUtilities.GetStringValueFromConfig("MyPhone1");
            string mobile1 = classUtilities.GetStringValueFromConfig("MyPhone2");
            string mobile2 = classUtilities.GetStringValueFromConfig("MyPhone4");
            string mobile3 = classUtilities.GetStringValueFromConfig("MyPhone9");

            DateTime today = DateTime.Today;
            //DateTime date1_p = new DateTime(today.Year, today.Month, 1).AddDays(dayValue1);
            
            while (exitThread == false)
            {
                try
                {
                    DateTime day_p = DateTime.Today;
                    hour = DateTime.Now.Hour;
                    minute = DateTime.Now.Minute;
                   
                    if (hour == hourValue && minute == minuteValue)
                    //if (1 == 1)//hhhh
                    {
                        //classThuNoFailLogWriter.WriteLog("----------------tao the-----------------");
                        //string cre_date = System.DateTime.Today.AddDays(-1).ToString("yyyyMMdd");
                        //string sms_date = System.DateTime.Today.AddDays(-1).ToString("dd/MM/yyy");
                        //table_cre_card = get_sum_create_card(cre_date);
                        //if (table_cre_card.Rows.Count > 0)
                        //{
                        //    string mess = Create_SMS_Crea_Card(table_cre_card, sms_date);
                        //    //SendSMSForCrea_Card(mobile, mess);
                        //    //SendSMSForCrea_Card(mobile1, mess);
                        //    //SendSMSForCrea_Card(mobile2, mess);
                        //    SendSMSForCrea_Card(mobile3, mess);//chi Van
                        //}

                        classThuNoFailLogWriter.WriteLog("----------------Begin Process-----------------");
                        _dataAccess = new classDataAccess();
                        table_duadate.Rows.Clear();
                        table_duadate = Get_CheckDueDate();
                        if (table_duadate.Rows.Count > 0)
                        {
                            foreach (DataRow row in table_duadate.Rows)
                            {
                                string due_date = row.ItemArray[0].ToString();
                                DateTime dua_date_p = new DateTime(int.Parse(due_date.Substring(0, 4)), int.Parse(due_date.Substring(4, 2)), int.Parse(due_date.Substring(6, 2)));                      
                                string get_date = dua_date_p.AddDays(-1).ToString("yyyyMMdd");
                                if (System.DateTime.Today == dua_date_p)//chi gui sms vao ngay dua date
                                //if(1==1)//hhhh
                                {
                                    table.Rows.Clear();
                                    table = Get_Thu_No_Fail(get_date);
                                    if (table.Rows.Count > 0)
                                    {
                                        //string month = due_date.Substring(4, 2) + "/" + due_date.Substring(2, 2);
                                        string month = dua_date_p.AddMonths(-1).ToString("MM/yy");
                                        Insert_SMSMessage(table, month);
                                    }
                                }
                            }
                        }                            

                        classThuNoFailLogWriter.WriteLog("----------------End Process-----------------");
                        Thread.Sleep(1000 * 60);// de khong lap lap lai vong lap lan 2 trong ngay
                    }
                    else
                    {
                        if ((hour == hourValue && minute > minuteValue) || (hour > hourValue))// da qua gio thuc thi, sleep de chuyen den ngay tiep theo
                        {
                            classThuNoFailLogWriter.WriteLog("((hour == hourValue && minute > minuteValue) || (hour > hourValue)) sleep(minute): " + (60 * (24 - (hour - hourValue + 1))));
                            Thread.Sleep(1000 * (60 * 60) * (24 - (hour - hourValue + 1)));

                        }
                        else
                        {
                            if (hour < (hourValue - 1))
                            {
                                classThuNoFailLogWriter.WriteLog("(hour < hourValue) sleep(miute): " + ((hourValue - hour - 1) * 60 - minute));
                                Thread.Sleep(1000 * 60 * ((hourValue - hour - 1) * 60 - minute + minuteValue - 1));
                            }
                            else
                            {
                                if (hour == (hourValue - 1))
                                {
                                    classThuNoFailLogWriter.WriteLog("(hour == (hourValue - 1)) sleep(minute): " + (60 - minute + minuteValue - 1));
                                    Thread.Sleep((1000 * 60) * (60 - minute + minuteValue - 1));
                                    if ((60 - minute + minuteValue - 1) == 0)//1 phut truoc gio gui tn
                                        Thread.Sleep(1000 * 10);
                                }
                                else//(hour==hourValue)
                                {
                                    if (minute < (minuteValue - 1))
                                    {
                                        classThuNoFailLogWriter.WriteLog("(minute <= (minuteValue - 1)) sleep(minute): " + (minuteValue - minute - 1));
                                        Thread.Sleep((1000 * 60) * (minuteValue - minute - 1));
                                    }
                                    else
                                    {
                                        classThuNoFailLogWriter.WriteLog("sleep(ss): " + 10);
                                        Thread.Sleep(1000 * 10);
                                    }
                                }
                            }
                        }
                    }
                    
                    
                }
                catch (Exception ex)
                {
                    classThuNoFailLogWriter.WriteLog("Error RunService(), " + ex.Message);
                }
            }
        }

        private static string Create_SMS_Crea_Card(DataTable table_cre_card, string date)
        {
            try
            {                
                string message="";
                message = date + ": ";
                foreach (DataRow row in table_cre_card.Rows)
                {
                    message = message + row.ItemArray[1].ToString() + "-" + row.ItemArray[2].ToString() + ", ";
                }
                return message;
            }
            catch (Exception ex)
            {
                classCheckDueDateLogWriter.WriteLog("Error Create_SMS_Crea_Card():" + ex.Message);
                return "";
            }
        }
        private static void SendSMSForCrea_Card(string mobile,  string sms)
        {
            try
            {
                string SMS_TYPE = "SMSME";               
                classDataAccess ebankDataAccess = new classDataAccess();
                ebankDataAccess.OpenConnection("EBANK_GW");

                

                ebankDataAccess.InsertSMSMessateToEBankGW_2(classDataAccess.IDALERT
                                                            , mobile //classUtilities.GetRandomMobile()
                                                            , sms
                                                            , 'N'//Y: (se ko gui tin nhan),//N: se gui tin nhan
                                                            , SMS_TYPE);
                ebankDataAccess.CloseConnection();
            }
            catch (Exception ex)
            {
                classCheckDueDateLogWriter.WriteLog("Error SendSMSForCrea_Card():" + ex.Message);
            }
        }
        private static DataTable Get_Thu_No_Fail(string due_date)
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
                    classThuNoFailLogWriter.WriteLog("Error Get_Thu_No_Fail(), " + ex.Message);
                    return null;
                }
                if (maxUpdateDT != null)
                {
                    table = _dataAccess.GetThuNoFail(due_date,maxUpdateDT);
                }
            }
            return table;
        }

        private static DataTable get_sum_create_card(string cre_dt)
        {
            DataTable table = new DataTable();
            try
            {
                table.Rows.Clear();
                table = _dataAccess.get_sum_create_card(cre_dt);
                return table;
            }
            catch (Exception ex)
            {
                classThuNoFailLogWriter.WriteLog("Error get_sum_create_card(), " + ex.Message);
                table.Clear();
                return table;
            }

        }

        private static DataTable Get_CheckDueDate()
        {
            DataTable table = new DataTable();
            try
            {
                table.Rows.Clear();
                table = _dataAccess.GetDueDate_DW();
                return table;
            }
            catch (Exception ex)
            {
                classThuNoFailLogWriter.WriteLog("Error Get_CheckDueDate(), " + ex.Message);
                table.Clear();
                return table;
            }

        }


        private static DataTable GetExpiredExtensionBoth()
        {
            DataTable table = new DataTable();
            try
            {
                table = _dataAccess.GetExpiredExtensionBoth();
            }
            catch (Exception ex)
            {
                classThuNoFailLogWriter.WriteLog("Error GetExpiredExtensionBoth(), " + ex.Message);
            }
            return table;
        }
        private static void Insert_SMSMessage(DataTable table, string month)
        {
            classDataAccess ebankDataAccess = new classDataAccess();
            classDataAccess dwDataAccess = new classDataAccess();
            ebankDataAccess.OpenConnection("EBANK_GW");
            dwDataAccess.OpenConnection("CW_DW");

            string message = "";
            int result = 0;
            int count = 0;
            int count_err = 0;
            //string expiredDate_P = DateTime.Today.Month.ToString()+"/"+DateTime.Today.Year.ToString();
            foreach (DataRow row in table.Rows)
            {
                result = 0;

                message = CreateSMSMessage(row.ItemArray[4].ToString(), row.ItemArray[0].ToString(), row.ItemArray[1].ToString(), month);

                if (string.IsNullOrEmpty(message) == false)
                {
                    string mobile = classUtilities.GetMobileFromCardNoOfSpecialList(row.ItemArray[0].ToString(), row.ItemArray[2].ToString());
                    if (row.ItemArray[2].ToString() == "khong co")
                    {
                        result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                            row.ItemArray[7].ToString()
                                                                            , mobile //row.ItemArray[2].ToString() //classDataAccess.MYPHONE
                                                                            , message
                                                                            , 'Y'//Y se ko gui tin nhan, N se gui tin nhan
                                                                            , SMS_TYPE);
                    }
                    else
                    {
                        result = ebankDataAccess.InsertSMSMessateToEBankGW_2(//classDataAccess.IDALERT
                                                                            row.ItemArray[7].ToString()
                                                                            , mobile //row.ItemArray[2].ToString() //classDataAccess.MYPHONE
                                                                            , message
                                                                            , 'N'//Y se ko gui tin nhan, N se gui tin nhan
                                                                            , SMS_TYPE);
                    }
                    if (result == 1)
                    {
                        if (row.ItemArray[2].ToString() == "khong co")
                        {
                            count += dwDataAccess.InsertThuNoFailToDW(SMS_TYPE, message
                                                                , row.ItemArray[2].ToString()//    ,long.Parse(row.ItemArray[10].ToString())
                                                                , DateTime.Now
                                //, row.ItemArray[0].ToString()
                                                                , row.ItemArray[8].ToString()
                                                                , row.ItemArray[4].ToString()
                                                                , row.ItemArray[3].ToString()
                                                                , row.ItemArray[1].ToString()
                                                                , "Y"
                                                                , row.ItemArray[9].ToString()
                                                                );
                        }
                        else
                        {
                            count += dwDataAccess.InsertThuNoFailToDW(SMS_TYPE, message
                                                                , row.ItemArray[2].ToString()//    ,long.Parse(row.ItemArray[10].ToString())
                                                                , DateTime.Now
                                //, row.ItemArray[0].ToString()
                                                                , row.ItemArray[8].ToString()
                                                                , row.ItemArray[4].ToString()
                                                                , row.ItemArray[3].ToString()
                                                                , row.ItemArray[1].ToString()
                                                                , "N"
                                                                , row.ItemArray[9].ToString()
                                                                );
                        }
                    }
                    else
                    {
                        count_err += dwDataAccess.InsertThuNoFailToDW(SMS_TYPE, message
                                                               , row.ItemArray[2].ToString()//    ,long.Parse(row.ItemArray[10].ToString())
                                                               , DateTime.Now
                            //, row.ItemArray[0].ToString()
                                                               , row.ItemArray[8].ToString()
                                                               , row.ItemArray[4].ToString()
                                                               , row.ItemArray[3].ToString()
                                                               , row.ItemArray[1].ToString()
                                                               , "E"
                                                               , row.ItemArray[9].ToString()
                                                               );
                    }
                }
            }
            ebankDataAccess.CloseConnection();
            dwDataAccess.CloseConnection();
            classThuNoFailLogWriter.WriteLog("Message da duoc Insert vao EbankGW thanh cong: " + count);
            classThuNoFailLogWriter.WriteLog("Message loi khong Insert vao EbankGW: " + count_err);
            return;
        }

        private static string CreateSMSMessage(string brand, string pan, string expiredDate, string month)
        {
            try
            {
                pan = pan.Substring(12, 4);
                expiredDate = expiredDate.Substring(4, 2) + "/" + expiredDate.Substring(0, 4);
                string smsMessage = "TK cua Quy khach khong du TT du no the tin dung ky sao ke "
                + month + "\nVui long nop them tien vao TK hoac TT bang hinh thuc khac\nBo qua tin nhan neu Quy khach da TT";

                //if (smsMessage.Length > 160)
                //    smsMessage = smsMessage.Substring(0, 160);
                return smsMessage;
            }
            catch (Exception ex)
            {
                classThuNoFailLogWriter.WriteLog("Error CreateSMSMessage(), " + ex.Message);
                return "";
            }
        }

    }
}
