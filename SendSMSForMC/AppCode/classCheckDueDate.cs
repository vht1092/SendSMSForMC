using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Windows.Forms;
using System.Threading;
using System.Data;
using System.Data.OracleClient;
using SendSMSForMC.AppCode;
using System.Globalization;
namespace SendSMSForMC.AppCode
{
    class classCheckDueDate
    {
        public static bool _exitThread = false;
        public static string _updateDateTime = null;

        private static classDataAccess _dataAccess = new classDataAccess();
        //private static List<string[]> _currencyMapping = new List<string[]>();
        //private static List<string[]> _specialCardList = new List<string[]>();
        //private static string DEFAULT_CRNCY_ALPA = "MTT";
        public static string SMS_TYPE1 = "TXNMSG";
        public static string SMS_TYPE2 = "MANPAY";

        private static string SCBPhone = "";
        
        public static void RunService()
        {
           try
           {
                int minute = 0;
                int hour = 0;            
               
                DataTable table = new DataTable();
                                           
                SCBPhone = classUtilities.GetStringValueFromConfig("SCB_Contact_Phone");

                while (_exitThread == false)
                {
                    minute = DateTime.Now.Minute;
                    hour = DateTime.Now.Hour;
                    int hourValue = classUtilities.GetIntValueFromConfig("CheckDueDate_Hour");
                    int minuteValue = classUtilities.GetIntValueFromConfig("CheckDueDate_Minute");
                    int hourValue2 = classUtilities.GetIntValueFromConfig("CheckDueDate_Hour2");
                    int minuteValue2 = classUtilities.GetIntValueFromConfig("CheckDueDate_Minute2");

                    if ((hour == hourValue && minute == minuteValue) || (hour == hourValue2 && minute == minuteValue2))
                    //if (1==1)//hhhh
                    {

                        classCheckDueDateLogWriter.WriteLog("----------------Begin Process-----------------");


                        _dataAccess = new classDataAccess();
                        table.Rows.Clear();                        
                        string mobile = classUtilities.GetStringValueFromConfig("MyPhone1");
                        string mobile2 = classUtilities.GetStringValueFromConfig("MyPhone2");
                        string mobile3 = classUtilities.GetStringValueFromConfig("MyPhone3");
                        string mobile4 = classUtilities.GetStringValueFromConfig("MyPhone4");
                        string mobile7 = classUtilities.GetStringValueFromConfig("MyPhone7");
                        string mobile10 = classUtilities.GetStringValueFromConfig("MyPhone10");
                        string mobile11 = classUtilities.GetStringValueFromConfig("MyPhone11");
                        double date1 = classUtilities.GetIntValueFromConfig("DueDate1");
                        double date2 = classUtilities.GetIntValueFromConfig("DueDate2");
                        double date3 = classUtilities.GetIntValueFromConfig("DueDate3");
                        double date4 = classUtilities.GetIntValueFromConfig("DueDate4");
                        double date5 = classUtilities.GetIntValueFromConfig("DueDate5");

                        table = Get_CheckDueDate();

                        foreach (DataRow row in table.Rows)
                        {
                            string due_date = row.ItemArray[0].ToString();
                            DateTime dua_date_p = new DateTime(int.Parse(due_date.Substring(0, 4)), int.Parse(due_date.Substring(4, 2)), int.Parse(due_date.Substring(6, 2)));
                            if (System.DateTime.Today == dua_date_p.AddDays(date1))
                            {
                                SendSMSForDueDateFirst(mobile);
                                SendSMSForDueDateFirst(mobile2);
                                SendSMSForDueDateFirst(mobile3);                                
                                SendSMSForDueDateFirst(mobile7);
                                SendSMSForDueDateFirst(mobile10);
                                SendSMSForDueDateFirst(mobile11);
                            }
                            if (
                                System.DateTime.Today == dua_date_p.AddDays(date2)
                                || System.DateTime.Today == dua_date_p.AddDays(date3) || System.DateTime.Today == dua_date_p.AddDays(date4)
                                || System.DateTime.Today == dua_date_p.AddDays(date5)
                                )
                            {
                                SendSMSForDueDate(mobile);
                                SendSMSForDueDate(mobile2);
                                SendSMSForDueDate(mobile3); 
                                SendSMSForDueDate(mobile7);
                                SendSMSForDueDate(mobile10);
                                SendSMSForDueDate(mobile11);
                            }
                        }
                        ////////////////////////////////////////////////
                        int date_value=classUtilities.GetIntValueFromConfig("date_backuplog");
                        if (System.DateTime.Today.Day == date_value)
                        {
                            SendSMSForSpaceDisk(mobile);
                            SendSMSForSpaceDisk(mobile2);
                            SendSMSForSpaceDisk(mobile3);
                            SendSMSForSpaceDisk(mobile7);
                            
                        }

                        classCheckDueDateLogWriter.WriteLog("----------------End Process----------------- at: " + DateTime.Now.ToString());

                        Thread.Sleep(1000 * 60);// de khong lap lap lai vong lap lan 2 trong ngay
                    }
                    else
                    {
                        //if ((hour == hourValue && minute > minuteValue) || (hour > hourValue))// da qua gio thuc thi, sleep de chuyen den ngay tiep theo
                        if ((hour == hourValue2 && minute > minuteValue2) || (hour > hourValue2))// da qua gio gui sms lan 2, sleep de chuyen den ngay tiep theo
                        {
                            classCheckDueDateLogWriter.WriteLog("((hour == hourValue && minute > minuteValue) || (hour > hourValue)) sleep(minute): " + (60 * (24 - (hour - hourValue + 1))));
                            Thread.Sleep(1000 * (60 * 60) * (24 - (hour - hourValue + 1)));

                        }
                        else
                        {
                            if (hour < (hourValue - 1))
                            {
                                classCheckDueDateLogWriter.WriteLog("(hour < hourValue) sleep(miute): " + ((hourValue - hour - 1) * 60 - minute));
                                Thread.Sleep(1000 * 60 * ((hourValue - hour - 1) * 60 - minute + minuteValue - 1));
                            }
                            else
                            {
                                if (hour == (hourValue - 1))
                                {
                                    classCheckDueDateLogWriter.WriteLog("(hour == (hourValue - 1)) sleep(minute): " + (60 - minute + minuteValue - 1));
                                    Thread.Sleep((1000 * 60) * (60 - minute + minuteValue - 1));
                                    if ((60 - minute + minuteValue - 1) == 0)//1 phut truoc gio gui tn
                                        Thread.Sleep(1000 * 10);
                                }
                                else
                                {
                                    if (hour == hourValue)
                                    {
                                        if (minute < (minuteValue - 1))
                                        {
                                            classCheckDueDateLogWriter.WriteLog("(minute <= (minuteValue - 1)) sleep(minute): " + (minuteValue - minute - 1));
                                            Thread.Sleep((1000 * 60) * (minuteValue - minute - 1));
                                        }
                                        if (minute > minuteValue)//qua gio gui sms lan 1, sleep de chuyen den lan 2
                                        {
                                            classCheckDueDateLogWriter.WriteLog("(minute > minuteValue): " + (60 * (hourValue2 - hour - 1)));
                                            Thread.Sleep(1000 * (60 * 60) * (hourValue2 - hour - 1));
                                        }
                                        else//minute = (minuteValue - 1)
                                        {
                                            classCheckDueDateLogWriter.WriteLog("sleep(ss): " + 10);
                                            Thread.Sleep(1000 * 10);
                                        }
                                    }
                                    else//(hour > hourValue)&&(hour < hourValue2)
                                    {
                                        if ((hour > hourValue) && (hour < (hourValue2 - 1)))
                                        {
                                            classCheckDueDateLogWriter.WriteLog("(hour > hourValue) && (hour < hourValue2): " + (60 * (hourValue2 - hour - 1)));
                                            Thread.Sleep(1000 * (60 * 60) * (hourValue2 - hour - 1));
                                        }
                                        else
                                        {
                                            if (hour == (hourValue2 - 1))
                                            {
                                                classCheckDueDateLogWriter.WriteLog("(hour == (hourValue2 - 1)) sleep(minute): " + (60 - minute + minuteValue2 - 1));
                                                Thread.Sleep((1000 * 60) * (60 - minute + minuteValue2 - 1));
                                                if ((60 - minute + minuteValue2 - 1) == 0)//1 phut truoc gio gui tn
                                                    Thread.Sleep(1000 * 10);
                                            }
                                            else
                                            {
                                                if (hour == hourValue2)
                                                {
                                                    if (minute < (minuteValue2 - 1))
                                                    {
                                                        classCheckDueDateLogWriter.WriteLog("(minute <= (minuteValue2 - 1)) sleep(minute): " + (minuteValue2 - minute - 1));
                                                        Thread.Sleep((1000 * 60) * (minuteValue2 - minute - 1));
                                                    }
                                                    if (minute > minuteValue2)//qua gio gui sms lan 2, sleep de chuyen den ngay tiep theo
                                                    {
                                                        classCheckDueDateLogWriter.WriteLog("(minute > minuteValue2): " + (60 * (24- (hourValue2 - hour + 1))));                                                        
                                                        Thread.Sleep(1000 * (60 * 60) * (24 - (hour - hourValue + 1)));
                                                    }
                                                    else//minute = (minuteValue2 - 1)
                                                    {
                                                        classCheckDueDateLogWriter.WriteLog("sleep(ss): " + 10);
                                                        Thread.Sleep(1000 * 10);
                                                    }
                                                }

                                            }
                                        }

                                    }

                                }
                            }
                        }
                    }




                }
                   
                    
                
        }
        catch (Exception ex)
        {
            classCheckDueDateLogWriter.WriteLog("Error RunService(), " + ex.Message);
        }
    }
        private static void SendSMSForDueDateFirst(string mobile)
        {
            try
            {
                classDataAccess ebankDataAccess = new classDataAccess();
                ebankDataAccess.OpenConnection("EBANK_GW");

                string SMS_TYPE = "SMSME";
                string message = "today is first due date! pls send reminder payment SMS and run instruction file!";

                ebankDataAccess.InsertSMSMessateToEBankGW(classDataAccess.IDALERT
                                                            , mobile //classUtilities.GetRandomMobile()
                                                            , message
                                                            , 'N'//Y: (se ko gui tin nhan),//N: se gui tin nhan
                                                            , SMS_TYPE);
                ebankDataAccess.CloseConnection();
            }
            catch (Exception ex)
            {
                classCheckDueDateLogWriter.WriteLog("Error SendSMSForDueDateFirst():" + ex.Message);
            }
        }
        private static void SendSMSForDueDate(string mobile)
        {
            try
            {
                classDataAccess ebankDataAccess = new classDataAccess();
                ebankDataAccess.OpenConnection("EBANK_GW");

                string SMS_TYPE = "SMSME";
                string message = "today is due date! pls run instruction file!";

                ebankDataAccess.InsertSMSMessateToEBankGW(classDataAccess.IDALERT
                                                            , mobile //classUtilities.GetRandomMobile()
                                                            , message
                                                            , 'N'//Y: (se ko gui tin nhan),//N: se gui tin nhan
                                                            , SMS_TYPE);
                ebankDataAccess.CloseConnection();
            }
            catch (Exception ex)
            {
                classCheckDueDateLogWriter.WriteLog("Error SendSMSForDueDate():" + ex.Message);
            }
        }

        private static void SendSMSForSpaceDisk(string mobile)
        {
            try
            {
                classDataAccess ebankDataAccess = new classDataAccess();
                ebankDataAccess.OpenConnection("EBANK_GW");             

                string SMS_TYPE = "SMSME";


                string message = "tomorrow will backup log switch at: /disk2/CW/CSWT/CWLOG/PREVLOGS. pls check space of disk2 on sever 76.11!";

                ebankDataAccess.InsertSMSMessateToEBankGW(classDataAccess.IDALERT
                                                            , mobile //classUtilities.GetRandomMobile()
                                                            , message
                                                            , 'N'//Y: (se ko gui tin nhan),//N: se gui tin nhan
                                                            , SMS_TYPE);
                ebankDataAccess.CloseConnection();
            }
            catch (Exception ex)
            {
                classCheckDueDateLogWriter.WriteLog("Error SendSMSForSpaceDisk():" + ex.Message);
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
                classCheckDueDateLogWriter.WriteLog("Error Get_CheckDueDate(), " + ex.Message);
                table.Clear();
                return table;
            }

        }


      
    }
    
}
