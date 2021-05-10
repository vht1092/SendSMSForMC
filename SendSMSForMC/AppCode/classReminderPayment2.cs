using System;
using System.Collections.Generic;
using System.Text;
using System.Windows.Forms;
using System.IO;
using System.Threading;
using System.Data;
using SendSMSForMC.AppCode;

namespace SendSMSForMC
{
    class classReminderPayment2
    {
        public static bool exitThread = false;

        private static classDataAccess _dataAccess = new classDataAccess();

        public static string SMS_TYPE = "DEBT02";

        private static string SCBPhone = "";

        public static void RunService()
        {
           
            int hourValue = 0;
            int minuteValue = 0;
            string settleMonth = "";
            int day = 0;
            int hour = 0;
            int minute = 0;
            DataTable table = new DataTable();

            //dayValue = GetProcessingDay();
            //string dueDate = GetDueDate();
            
            //SCBPhone = classUtilities.GetStringValueFromConfig("SCB_Contact_Phone");
            hourValue = classUtilities.GetIntValueFromConfig("Reminder_Outstanding_2_Hour");
            minuteValue = classUtilities.GetIntValueFromConfig("Reminder_Outstanding_2_Minute");
            string due_date=classUtilities.GetIntValueFromConfig("Reminder_Outstanding_2_due_date").ToString();
            settleMonth = DateTime.Now.AddMonths(-1).ToString("yyyyMM");            
            
            while (exitThread == false)
            {
                day = DateTime.Now.Day ;
                hour = DateTime.Now.Hour;
                minute = DateTime.Now.Minute;
                string date = DateTime.Now.AddDays(2).ToString("yyyyMMdd");
                if(1==1)//hhhh
                //if(hour == hourValue && minute == minuteValue)                        
                {
                    try
                    {
                        classReminderPayment2LogWriter.WriteLog("----------------Begin Process-----------------");
                        _dataAccess = new classDataAccess();
                        table.Rows.Clear();
                        table = Get_Reminder_Payment_2(settleMonth, due_date);                        
                        if (table.Rows.Count > 0)
                            Insert_SMSMessage(table);
                        classReminderPayment2LogWriter.WriteLog("----------------End Process-----------------");
                        Thread.Sleep(1000 * 3600 * 24 * 23);// sleep 1 phut de troi qua thoi gian lap lai
                    }
                    catch (Exception ex)
                    {
                        classReminderPayment2LogWriter.WriteLog("Error RunService(), " + ex.Message);
                    }
                }
                Thread.Sleep(1000 * 50);//1 gio moi check 1 lan
            }
        }

       
        private static DataTable Get_Reminder_Payment_2(string settleMonth, string p_due_dt)
        {
            DataTable table = new DataTable();
            try
            {
                //table = _dataAccess.GetReminderPayment2(settleMonth);
                table = _dataAccess.GetReminderPayment2_IPP(settleMonth,p_due_dt);
            }
            catch (Exception ex)
            {
                classReminderPayment2LogWriter.WriteLog("Error Get_Reminder_Payment_2(), " + ex.Message);
            }
            return table;
        }
        
        private static string GetDueDate()
        {
            DataTable table = Get_Due_Date();
            string dueDate = "";
            try
            {
                dueDate = table.Rows[0].ItemArray[0].ToString();
            }
            catch (Exception ex)
            {
                classReminderPayment2LogWriter.WriteLog("Error Get_Reminder_Payment_2(), " + ex.Message);
            }
            return dueDate;
        }

        private static DataTable Get_Due_Date()
        {
            DataTable table = new DataTable();
            try
            {
                table = _dataAccess.GetDueDate();
            }
            catch (Exception ex)
            {
                classReminderPayment2LogWriter.WriteLog("Error Get_Reminder_Payment_2(), " + ex.Message);
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

            foreach (DataRow row in table.Rows)
            {
                result = 0;
                message = CreateSMSMessage(row.ItemArray[4].ToString(), row.ItemArray[2].ToString(), row.ItemArray[5].ToString(),
                                          row.ItemArray[6].ToString(), row.ItemArray[7].ToString(), row.ItemArray[8].ToString(),
                                          row.ItemArray[10].ToString(), row.ItemArray[11].ToString(), row.ItemArray[12].ToString()
                                          ,row.ItemArray[13].ToString());
                if (string.IsNullOrEmpty(message) == false)
                {
                    //mobile = classUtilities.GetMobileFromCardNoOfSpecialList(row.ItemArray[2].ToString(), row.ItemArray[1].ToString());
                    if (row.ItemArray[1].ToString() == "khong co")
                    {
                        result = ebankDataAccess.InsertSMSMessateToEBankGW_2(classDataAccess.IDALERT
                                                           , row.ItemArray[1].ToString()// classDataAccess.MYPHONE
                                                           , message
                                                           , 'Y'//Sent (Y se ko gui tin nhan)
                                                           , SMS_TYPE);
                    }
                    else
                    {
                        result = ebankDataAccess.InsertSMSMessateToEBankGW_2(classDataAccess.IDALERT
                                                            , row.ItemArray[1].ToString()// classDataAccess.MYPHONE
                                                            , message
                                                            , 'N'//hhhh Sent (N se gui tin nhan, Y se ko gui tin nhan)
                                                            , SMS_TYPE);
                    }
                    if (result == 1)
                    {
                        if (row.ItemArray[1].ToString() == "khong co")
                        {
                            count += dwDataAccess.InsertReminderPayment_2SMSToDW(SMS_TYPE, message
                                                                , row.ItemArray[1].ToString()//    ,long.Parse(row.ItemArray[10].ToString())
                                                                , DateTime.Parse(row.ItemArray[0].ToString())
                                                                , row.ItemArray[9].ToString()
                                                                , row.ItemArray[4].ToString()
                                                                , row.ItemArray[3].ToString()
                                                                , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                                , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                                , row.ItemArray[7].ToString()//    , long.Parse(row.ItemArray[7].ToString())
                                                                , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                                                                , "Y"
                                                                );
                        }
                        else
                        {
                            count += dwDataAccess.InsertReminderPayment_2SMSToDW(SMS_TYPE, message
                                                               , row.ItemArray[1].ToString()//    ,long.Parse(row.ItemArray[10].ToString())
                                                               , DateTime.Parse(row.ItemArray[0].ToString())
                                                               , row.ItemArray[9].ToString()
                                                               , row.ItemArray[4].ToString()
                                                               , row.ItemArray[3].ToString()
                                                               , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                               , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                               , row.ItemArray[7].ToString()//    , long.Parse(row.ItemArray[7].ToString())
                                                               , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                                                               , "N" //hhhh (N se gui tin nhan, Y se ko gui tin nhan)
                                                               );
                        }
                    }
                }
            }
            ebankDataAccess.CloseConnection();
            dwDataAccess.CloseConnection();
            classReminderPayment2LogWriter.WriteLog("So luong message da duoc Insert vao EbankGW thanh cong: " + count);
            return;
        }

        private static string CreateSMSMessage(string brand, string pan, string settleDay, string closingBal
        , string miniPay, string dueDay, string tol_bal_ipp, string vip_card, string vip_cif, string sum_pays_cw)
        {
            try
            {
                pan = pan.Substring(12, 4);
                string settleDay_f=null;
                if(brand=="VISA")
                    settleDay_f= "15/" + settleDay.Substring(4, 2) + "/" + settleDay.Substring(2, 2);
                else
                    settleDay_f = "25/" + settleDay.Substring(4, 2) + "/" + settleDay.Substring(2, 2);

                //dueDay = dueDay.Substring(6, 2) + "/" + dueDay.Substring(4, 2) + "/" + dueDay.Substring(2, 2);
                dueDay = dueDay.Substring(6, 2) + "/" + dueDay.Substring(4, 2);
                if (vip_card == "Y" || vip_cif == "Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";
                double clsBal = 0;// double.Parse(closingBal); 
                double min = 0;// double.Parse(miniPay);
                //if (double.Parse(closingBal) < 0 || double.Parse(sum_pays_cw) < 0)
                if (double.Parse(closingBal) < 0 )
                {
                    if (tol_bal_ipp != "")
                    {
                        //clsBal += double.Parse(tol_bal_ipp);
                        //min += double.Parse(tol_bal_ipp);
                        clsBal = double.Parse(tol_bal_ipp);
                        min = double.Parse(tol_bal_ipp);
                    }                    
                }
                else
                {
                    if (double.Parse(sum_pays_cw) < 0)// closing > 0 va sum_pays_cw < 0
                    {
                        clsBal = double.Parse(closingBal) + double.Parse(tol_bal_ipp);
                        min = double.Parse(tol_bal_ipp);
                    }
                    else
                    {
                        clsBal = double.Parse(closingBal) + double.Parse(tol_bal_ipp);
                        min = double.Parse(miniPay) + double.Parse(tol_bal_ipp);
                    }
                }
                string cloBal = string.Format("{0:#,##0.##}", clsBal);
                string miniPayment = string.Format("{0:#,##0.##}", min);
                
                //string smsMessage = "Cam on Quy khach da su dung the SCB " + brand + " x" + pan + ".Tong so tien TT: "
                //+ cloBal + "VND" + ".Vui long TT toi thieu: " + miniPayment + "VND" + " truoc " + dueDay + ".Chi tiet LH: " + SCBPhone;
                string smsMessage = "Cam on Quy khach su dung the " + pan +"\nDu no den "+settleDay_f+": "+ cloBal + "VND\nTT toi thieu: " + miniPayment + "VND" + "\nNgay den han " + dueDay + "\nVui long bo qua neu da TT\nLH " + SCBPhone;

                //if (smsMessage.Length > 160)
                //    smsMessage = smsMessage.Substring(0, 160);
                return smsMessage;
            }
            catch (Exception ex)
            {
                classReminderPayment2LogWriter.WriteLog(ex.Message);
                return "";
            }
        }
    }
}
