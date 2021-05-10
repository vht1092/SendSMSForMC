using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Windows.Forms;
using System.Threading;

namespace SendSMSForMC
{
    class classGDMCDebitLogWriter
    {
        public static FileStream fs;
        public static StreamWriter w;

        public classGDMCDebitLogWriter()
        {
            fs = null;
            w = null;
        }

        public static void OpenFileWriter()
        {
            string rootpath = Application.StartupPath;
            string filename = rootpath + "\\log\\" + "GDMCDebit_" + DateTime.Today.ToString("yyyyMMdd") + ".log";
            try
            {
                fs = new FileStream(filename, FileMode.Append);
                w = new StreamWriter(fs, Encoding.ASCII);
            }
            catch (Exception ex)
            {
                WriteLog(ex.Message);
                return;
            }
        }

        public static void CloseFileWriter()
        {
            if (w != null)
            {
                w.Close();
            }
            if (fs != null)
            {
                fs.Close();
            }
        }

        public static void WriteLog(string content)
        {
            OpenFileWriter();
            content = DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss ") + content;
            w.WriteLine(content);
            w.Flush();
            CloseFileWriter();
        }

        //public static List<string[]> ReadMappingFile(string filename)
        //{
        //    try
        //    {
        //        string rootpath = Application.StartupPath;
        //        filename = rootpath + "\\" + filename;
        //        FileStream fs = new FileStream(filename, FileMode.Open);
        //        StreamReader r = new StreamReader(fs, Encoding.ASCII);
        //        List<string[]> data = new List<string[]>();
        //        int i = 0;
        //        while (r.EndOfStream == false)
        //        {
        //            data.Add(r.ReadLine().Split('|'));
        //        }
        //        r.Close();
        //        fs.Close();
        //        return data;
        //    }
        //    catch (Exception ex)
        //    {
        //        classGDMCDebitLogWriter.WriteLog("Error ReadCurrencyMappingFile(), " + ex.Message);
        //    }
        //    return null;
        //}
    }
}
