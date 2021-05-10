using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading;
using System.Windows.Forms;
using System.Data;
using SendSMSForMC.AppCode;

namespace SendSMSForMC
{
    class classReminderPayment1
    {
        public static bool exitThread = false;
        
        private static classDataAccess _dataAccess = new classDataAccess();

        public static string SMS_TYPE = "DEBT01";

        private static string SCBPhone = "";

        public static void RunService()
        {
            int dayValue = 0;
            int hourValue = 0;
            int minuteValue = 0;

            string settleMonth = "";
            string VS_MS_Checking="";
            int day = 0;
            int hour = 0;
            int minute = 0;            
            DataTable table = new DataTable();

            //SCBPhone = classUtilities.GetStringValueFromConfig("SCB_Contact_Phone");
            dayValue = classUtilities.GetIntValueFromConfig("Reminder_Outstanding_1_Day");
            hourValue = classUtilities.GetIntValueFromConfig("Reminder_Outstanding_1_Hour");
            minuteValue = classUtilities.GetIntValueFromConfig("Reminder_Outstanding_1_Minute");

            while (exitThread == false)
            {
                settleMonth = DateTime.Now.Year.ToString() + DateTime.Now.Month.ToString().PadLeft(2,'0');             
                day = DateTime.Now.Day;
                hour = DateTime.Now.Hour;
                minute = DateTime.Now.Minute;
                if(day > 15 && day < 25)
                    VS_MS_Checking="VS";
                else
                    VS_MS_Checking="MC";
                //if (day == dayValue && hour == hourValue && minute == minuteValue)//hhhh
                if(1==1)//hhhh
                {
                    try
                    {
                        classReminderPayment1LogWriter.WriteLog("----------------Begin Process-----------------");
                        _dataAccess = new classDataAccess();
                        table.Rows.Clear();
                        table = Get_Reminder_Payment_1(settleMonth, VS_MS_Checking);
                        if (table.Rows.Count > 0)
                            Insert_SMSMessage(table, VS_MS_Checking);

                        classReminderPayment1LogWriter.WriteLog("----------------End Process-----------------");
                        Thread.Sleep(1000 * 3600 * 24 * 23);// sleep 10 ngay sau khi thuc hien xong
                    }
                    catch(Exception ex)
                    {
                        classReminderPayment1LogWriter.WriteLog("Error RunService(), " + ex.Message);
                    }
                }
                Thread.Sleep(1000 * 50); // 1 gio moi check 1 lan
            }
        }

        private static DataTable Get_Reminder_Payment_1(string settleMonth, string crd_brn)
        {
            DataTable table = new DataTable();
            try
            {
                table = _dataAccess.GetReminderPayment1(settleMonth, crd_brn);
            }
            catch (Exception ex)
            {
                classReminderPayment1LogWriter.WriteLog("Error Get_Reminder_Payment_1(), " + ex.Message);
            }
            return table;
        }

        private static void Insert_SMSMessage(DataTable table, string crd_brn)
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
                                          row.ItemArray[6].ToString(), row.ItemArray[7].ToString(), row.ItemArray[8].ToString(), row.ItemArray[10].ToString(), crd_brn, row.ItemArray[11].ToString(), row.ItemArray[12].ToString());
                if (string.IsNullOrEmpty(message) == false)
                {
                    //mobile = classUtilities.GetMobileFromCardN oOfSpecialList(row.ItemArray[2].ToString(), row.ItemArray[1].ToString());
                    double ipp = 0;
                    if(row.ItemArray[10].ToString()!="")
                        ipp=double.Parse(row.ItemArray[10].ToString());
                    if (double.Parse(row.ItemArray[6].ToString()) >= 0 || double.Parse(row.ItemArray[6].ToString()) <= -100000 || ipp > 0)
                    {
                        if (row.ItemArray[1].ToString() == "khong co")
                        {
                            result = ebankDataAccess.InsertSMSMessateToEBankGW_2(classDataAccess.IDALERT
                                                                , row.ItemArray[1].ToString()//classDataAccess.MYPHONE
                                                                , message
                                                                , 'Y'// Y: se ko gui tin nhan, D: ko gui, N: gui, E:Error
                                                                , SMS_TYPE);
                        }
                        else
                        {
                            result = ebankDataAccess.InsertSMSMessateToEBankGW_2(classDataAccess.IDALERT
                                                                , row.ItemArray[1].ToString()//classDataAccess.MYPHONE
                                                                , message
                                                                , 'N'// hhhh Y: se ko gui tin nhan, D: ko gui, N: gui, E:Error
                                                                , SMS_TYPE);
                        }
                    }
                    else // 0 > closing > -100000 and don't have IPP, don't send
                    {
                        count += dwDataAccess.InsertReminderPayment_1SMSToDW(SMS_TYPE, message
                                                               , row.ItemArray[1].ToString()//    ,long.Parse(row.ItemArray[10].ToString())
                                                               , DateTime.Parse(row.ItemArray[0].ToString())
                                                               , row.ItemArray[9].ToString()
                                                               , row.ItemArray[4].ToString()
                                                               , row.ItemArray[3].ToString()
                                                               , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                               , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                               , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                                               , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                                                               , "Y"
                                                               );
                    }
                    if (result == 1)
                    {
                        if (row.ItemArray[1].ToString() == "khong co")
                        {
                            count += dwDataAccess.InsertReminderPayment_1SMSToDW(SMS_TYPE, message
                                                                , row.ItemArray[1].ToString()//    ,long.Parse(row.ItemArray[10].ToString())
                                                                , DateTime.Parse(row.ItemArray[0].ToString())
                                                                , row.ItemArray[9].ToString()
                                                                , row.ItemArray[4].ToString()
                                                                , row.ItemArray[3].ToString()
                                                                , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                                , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                                , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                                                , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                                                                , "Y"
                                                                );
                        }
                        else
                        {
                            count += dwDataAccess.InsertReminderPayment_1SMSToDW(SMS_TYPE, message
                                                                , row.ItemArray[1].ToString()//    ,long.Parse(row.ItemArray[10].ToString())
                                                                , DateTime.Parse(row.ItemArray[0].ToString())
                                                                , row.ItemArray[9].ToString()
                                                                , row.ItemArray[4].ToString()
                                                                , row.ItemArray[3].ToString()
                                                                , row.ItemArray[5].ToString()//    , long.Parse(row.ItemArray[5].ToString())
                                                                , row.ItemArray[6].ToString()//    , long.Parse(row.ItemArray[6].ToString())
                                                                , double.Parse(row.ItemArray[7].ToString())//    , long.Parse(row.ItemArray[7].ToString())
                                                                , row.ItemArray[8].ToString()//    , int.Parse(row.ItemArray[8].ToString())
                                                                , "N"//hhhh Y: se ko gui tin nhan, D: ko gui, N: gui, E:Error
                                                                );
                        }

                    }
                }
            }
            ebankDataAccess.CloseConnection();
            dwDataAccess.CloseConnection();
            classReminderPayment1LogWriter.WriteLog("So luong message da duoc Insert vao EbankGW thanh cong: " + count);
            return;
        }

        private static string CreateSMSMessage(string brand, string pan, string settleDay, string closingBal
                                    , string miniPay, string dueDay, string tol_bal_ipp, string crd_brn, string vip_card, string vip_cif)
        {
            try
           {
                pan = pan.Substring(12, 4);

                //settleDay = settleDay.Substring(4, 2) + "/" + settleDay.Substring(0, 4);
                settleDay = settleDay.Substring(4, 2) + "/" + settleDay.Substring(2, 2);
                //dueDay = dueDay.Substring(6, 2) + "/" + dueDay.Substring(4, 2) + "/" + dueDay.Substring(2, 2);
                dueDay = dueDay.Substring(6, 2) + "/" + dueDay.Substring(4, 2);

                double clsBal = double.Parse(closingBal);
                double mini = double.Parse(miniPay);
                if (double.Parse(closingBal) < 0 && tol_bal_ipp != "")
                {
                    if (double.Parse(tol_bal_ipp) > 0)
                    {
                        clsBal = double.Parse(tol_bal_ipp);
                        mini = double.Parse(tol_bal_ipp);
                    }
                   
                }
                if (double.Parse(closingBal) >= 0 && tol_bal_ipp != "")
                {
                    clsBal += double.Parse(tol_bal_ipp);
                    mini += double.Parse(tol_bal_ipp);
                }

                //if (double.Parse(closingBal) > 0)
                //{
                //    clsBal = double.Parse(closingBal);
                //    mini = double.Parse(miniPay);
                //}
                //if (tol_bal_ipp != "")
                //{
                //    clsBal += double.Parse(tol_bal_ipp);
                //    mini += double.Parse(tol_bal_ipp);
                //}

                string cloBal = string.Format("{0:#,##0.##}", clsBal);                
                string miniPayment = string.Format("{0:#,##0.##}", mini);               
                string smsMessage = "";

                if (vip_card == "Y" || vip_cif == "Y")
                    SCBPhone = "1800545438";
                else
                    SCBPhone = "19006538";


                
                if( clsBal < 0)
                {
                    
                    double clsBal_1 = -1 * clsBal;
                    string cloBal_2 = string.Format("{0:#,##0.##}", clsBal_1);
                    if (crd_brn == "VS")
                        smsMessage = "Cam on Quy khach da su dung the SCB X" + pan + "\nSo DU CO trong the den 15/" + settleDay + ":" + cloBal_2 + "VND\nChi tiet LH: " + SCBPhone;
                    else
                        smsMessage = "Cam on Quy khach da su dung the SCB X" + pan + "\nSo DU CO trong the den 25/" + settleDay + ":" + cloBal_2 + "VND\nChi tiet LH: " + SCBPhone;
           

                }
                else
                    if (clsBal == 0)
                    {
                       
                        if (crd_brn == "VS")
                            smsMessage = "Cam on Quy khach da su dung va thanh toan the SCB " + brand + " X" + pan + "\nDu no den 15/" + settleDay + ": 0VND.\nChi tiet LH: " + SCBPhone;
                        else
                            smsMessage = "Cam on Quy khach da su dung va thanh toan the SCB " + brand + " X" + pan + "\nDu no den 25/" + settleDay + ": 0VND.\nChi tiet LH: " + SCBPhone;
                    }
                    else
                    {

                       
                        if (crd_brn == "VS")
                            smsMessage = "Cam on Quy khach su dung the " + pan + "\nDu no den 15/" + settleDay + ": " + cloBal + "VND\nTT toi thieu "
                                                + miniPayment + "VND\nNgay den han " + dueDay + "\nVui long bo qua neu da TT\nLH " + SCBPhone;
                        else
                            smsMessage = "Cam on Quy khach su dung the " + pan + "\nDu no den 25/" + settleDay + ": " + cloBal + "VND\nTT toi thieu "
                                                + miniPayment + "VND\nNgay den han " + dueDay + "\nVui long bo qua neu da TT\nLH " + SCBPhone;
                    }

 
                //if (smsMessage.Length > 160)
                //    smsMessage = smsMessage.Substring(0, 160);
                return smsMessage;
            }
            catch (Exception ex)
            {
                classReminderPayment1LogWriter.WriteLog("Error CreateSMSMessage(), " + ex.Message);
                return "";
            }
        }
    }
}
