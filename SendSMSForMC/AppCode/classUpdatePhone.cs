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
    class classUpdatePhone
    {
        public static bool _exitThread = false;
        public static string _updateDateTime = null;

        private static classDataAccess _dataAccess = new classDataAccess();

        public static string UPDATE_TYPE = "UP_INF";

        private static string SCBPhone = "";

        public static void RunService()
        {

            int minute = 0;
            int value = 0;
            DataTable table = new DataTable();
            SCBPhone = classUtilities.GetStringValueFromConfig("SCB_Contact_Phone");
            value = classUtilities.GetIntValueFromConfig("UpdatePhone_Minute");
            while (_exitThread == false)
            {
                try
                {
                    minute = DateTime.Now.Minute;

                    if (minute % value == 0)
                    //if(1==1)//hhhh
                    {
                      
                        classUpdatePhoneLogWriter.WriteLog("----------------Begin Process-----------------");
                        _dataAccess = new classDataAccess();
                        table.Rows.Clear();
                        table = Get_UpdatePhone();
                        if (table.Rows.Count > 0)
                            Process_Update_Phone(table);

                        classUpdatePhoneLogWriter.WriteLog("----------------End Process-----------------");
                      
                    }
                    if (value > 2)
                    {
                        if ((value - (minute % value) - 1) > 0)
                        {
                            classUpdatePhoneLogWriter.WriteLog("sleep " + (value - (minute % value) - 1) + " minute");
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
                    classUpdatePhoneLogWriter.WriteLog("Error RunService(), " + ex.Message);
                }
            }
        }

        private static DataTable Get_UpdatePhone()
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
                maxUpdateDT = _dataAccess.Get_Max_UpdateTime_Up(UPDATE_TYPE);
                //DataTable updateTime = _dataAccess.Get_Max_OutBal_UpdateTime(SMS_TYPE);
                //if (updateTime.Rows.Count == 1 || updateTime.Rows.Count == 2)
                //    maxUpdateDT = updateTime.Rows[0].ItemArray[0].ToString();
            }
            if (string.IsNullOrEmpty(maxUpdateDT) == false)
            {
                try
                {
                    long maxUpdateTime = long.Parse(maxUpdateDT);
                }
                catch (Exception ex)
                {
                    classUpdatePhoneLogWriter.WriteLog("Error Get_UpdatePhone(), " + ex.Message);
                    return null;
                }
                if (maxUpdateDT != null)
                    table = _dataAccess.GetUpdatePhone(maxUpdateDT);
            }
            return table;
        }


        private static void Process_Update_Phone(DataTable table)
        {
           
            classDataAccess dwDataAccess = new classDataAccess();        
            dwDataAccess.OpenConnection("CW_DW");
      
            int result = 0;
            int resu_aud1 = 0;
            int resu_aud2 = 0;
            int count = 0;
            int count_err = 0; 
            string phone_p="";
          

            foreach (DataRow row in table.Rows)
            {
                result = 0;
                if (dwDataAccess.Check_Exist_Card(row.ItemArray[1].ToString()) == "1")//cif co tren cw
                {
                  
                    phone_p=dwDataAccess.Get_Inf_Cus(row.ItemArray[1].ToString());//phone from CW
                    //if (phone_p != "")
                    {
                        resu_aud1 = dwDataAccess.Insert_UPD_INF_AUD(UPDATE_TYPE, row.ItemArray[1].ToString(), phone_p, row.ItemArray[0].ToString(),1);
                        resu_aud2 = dwDataAccess.Insert_UPD_INF_AUD(UPDATE_TYPE, row.ItemArray[1].ToString(), row.ItemArray[2].ToString(),row.ItemArray[0].ToString(), 2);
                        
                    }


                    result = dwDataAccess.Update_CIF(row.ItemArray[1].ToString(), row.ItemArray[2].ToString(), row.ItemArray[3].ToString(), row.ItemArray[0].ToString());
                    
                    if (result == 1)//update cif thanh cong
                    {
                        int flag_in_dw = dwDataAccess.Insert_Update_InfDW(UPDATE_TYPE, row.ItemArray[1].ToString(), row.ItemArray[2].ToString(), //row.ItemArray[3].ToString(), 
                            row.ItemArray[0].ToString(), "S");
                        if (flag_in_dw != 0)//insert DW thanh cong
                            count++;

                    }
                    else
                    {
                        classUpdatePhoneLogWriter.WriteLog("cif: " + row.ItemArray[1].ToString() + " can't update from FCC");
                        count_err++;
                    }
                }
                else
                {
                    dwDataAccess.Insert_Update_InfDW(UPDATE_TYPE, row.ItemArray[1].ToString(), row.ItemArray[2].ToString(), //row.ItemArray[3].ToString(), 
                        row.ItemArray[0].ToString(), "D");//D: cif don't have on CW
                }



            }
       
            dwDataAccess.CloseConnection();
            classUpdatePhoneLogWriter.WriteLog("so cif da update thanh cong: " + count);
            classUpdatePhoneLogWriter.WriteLog("so cif update loi: " + count_err);
            
        
        }


      
    }
}
