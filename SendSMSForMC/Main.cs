using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Text;
using System.Windows.Forms;
using System.Threading;
using SendSMSForMC.AppCode;

namespace SendSMSForMC
{
    public partial class Main : Form
    {
        Thread _threadThayDoiDuNo;
        Thread _threadTBGiaHanThe;
        Thread _threadNhacNoLan1;
        Thread _threadNhacNoLan2;
        Thread _threadThanhToanDuNo_TuDong;
        Thread _threadThanhToanDuNo_TaiQuay;
        Thread _threadTBDiemThuong;
        Thread _threadGDHoanTra;
        Thread _threadGDTangTien;
        Thread _threadKichHoatThe;
        Thread _threadNoQuaHan;
        Thread _threadCheckDueDate;
        Thread _threadPhiThuongNien;
        Thread _threadGDMCDebit;
        Thread _threadBlockAndFailFee;
        Thread _threadThuNoFail;
        Thread _threadDKIPP;
        Thread _threadThuNoTatToanIPP;
        Thread _threadUpdatePhone;
        Thread _threadCPGD;
   
   

//        string[][] _currencyMapping = new string[300][];

        public Main()
        {
            InitializeComponent();
            classUtilities.GetMappingFile();
            InitializeControl();
            //OpenLogWriter();
            InitializeThread();
            classDataAccess dataAccess = new classDataAccess();
        }

        private void InitializeThread()
        {
            _threadThayDoiDuNo = new Thread(new ThreadStart(classOutstandingChange.RunService));
            _threadTBGiaHanThe = new Thread(new ThreadStart(classExpiredExtension.RunService));
            _threadNhacNoLan1 = new Thread(new ThreadStart(classReminderPayment1.RunService));
            _threadNhacNoLan2 = new Thread(new ThreadStart(classReminderPayment2.RunService));
            _threadThanhToanDuNo_TuDong = new Thread(new ThreadStart(classOutstandingBalancePaymentAuto.RunService));
            _threadThanhToanDuNo_TaiQuay = new Thread(new ThreadStart(classOutstandingBalancePaymentManual.RunService));
            _threadTBDiemThuong = new Thread(new ThreadStart(classAccumulativeRewardPoint.RunService));
            _threadGDHoanTra = new Thread(new ThreadStart(classGDHoanTra.RunService));
            _threadGDTangTien = new Thread(new ThreadStart(classGDTangTien.RunService));
            _threadKichHoatThe = new Thread(new ThreadStart(classKichHoatThe.RunService));
            _threadNoQuaHan=new Thread(new ThreadStart(classNoQuaHan.RunService));
            _threadCheckDueDate = new Thread(new ThreadStart(classCheckDueDate.RunService));
            _threadPhiThuongNien = new Thread(new ThreadStart(classAnnual_Fee.RunService));
            _threadGDMCDebit = new Thread(new ThreadStart(classGDMCDebit.RunService));
            _threadBlockAndFailFee = new Thread(new ThreadStart(classBlockAndFailAnnualFee.RunService));
            _threadThuNoFail = new Thread(new ThreadStart(classThuNoFail.RunService));
            _threadDKIPP = new Thread(new ThreadStart(classDKIPP.RunService));
            _threadThuNoTatToanIPP = new Thread(new ThreadStart(classThuNoTatToanIPP.RunService));
            _threadUpdatePhone = new Thread(new ThreadStart(classUpdatePhone.RunService));
            _threadCPGD = new Thread(new ThreadStart(classCapPhepGD.RunService));
        }

        private void InitializeControl()
        {
            btnStopThayDoiDuNo.Enabled = false;
            btnStopTBGiaHanThe.Enabled = false;
            btnStopNhacNoLan1.Enabled = false;
            btnStopNhacNoLan2.Enabled = false;
            btnStopThanhToanDuNo_TuDong.Enabled = false;
            btnStopThanhToanDuNo_TaiQuay.Enabled = false;
            btnStopTBTichLuyDiemThuong.Enabled = false;
            btnStopGDHoanTra.Enabled = false;
            btnStopPhiThuongNien.Enabled = false;
            btnStopGDTangTien.Enabled = false;           
            btnStopGDMCDebit.Enabled = false;
            btnStopCheckDueDate.Enabled = false;
            btnStopBlockAndFailAnnualFee.Enabled = false;
            btnStopThuNoFail.Enabled = false;
            btnStopDKIPP.Enabled = false;
            btnStopThuNoTatToan.Enabled = false;
            btnStopUpdatePhone.Enabled = false;
            btnStartUpdatePhone.Enabled = false;//hhhh
            btnStartThanhToanDuNo_TuDong.Enabled = false;
            btnStartPhiThuongNien.Enabled = false;
            //btnStartGDTangTien.Enabled = false;
            btnStartUpdatePhone.Enabled = false;
            btnStartCPGD.Enabled = false;
            //btnStartNhacNoLan1.Enabled = false;//1111
            btnStartNhacNoLan2.Enabled = false;//1111
            
            

            txtThayDoiDuNo_BeginDateTime.Enabled = false;
            ckbThayDoiDuNo_.Checked = false;
            txtThanhToanTaiQuay_BeginDateTime.Enabled = false;
            ckbThanhToanTaiQuay.Checked = false;
            txtThanhToanTuDong_BeginDateTime.Enabled = false;
            ckbThanhToanTuDong.Checked = false;
            txtPhiThuongNien_BeginDateTime.Enabled = false;
            ckbThuNoFail_.Checked = false;
            //txtDKIPP_BeginDateTime.Enabled = false;
            ckbDKIPP.Checked = false;
            ckbUpdatePhone.Checked = false;
            ckbCPGD.Checked = false;
           

            
            
        }

        //private void OpenLogWriter()
        //{
        //    classExpiredExtensionLogWriter.OpenFileWriter();
        //    classReminderPayment1LogWriter.OpenFileWriter();
        //    classReminderPayment2LogWriter.OpenFileWriter();
        //    classOutstandingChangeLogWriter.OpenFileWriter();
        //    classOutstandingBalancePaymentAutoLogWriter.OpenFileWriter();
        //    classOutstandingBalancePaymentManualLogWriter.OpenFileWriter();
        //    classAccumulativeRewardPointLogWriter.OpenFileWriter();
        //    classDataAccessLogWriter.OpenFileWriter();
        //}

        private void btnStartTBGiaHanThe_Click(object sender, EventArgs e)
        {
            classExpiredExtensionLogWriter.WriteLog("Start Service TB Gia Han The");

            btnStartTBGiaHanThe.Enabled = false;
            btnStopTBGiaHanThe.Enabled = true;

            switch (_threadTBGiaHanThe.ThreadState)
            {
                case ThreadState.Unstarted:
                    _threadTBGiaHanThe.Start();
                    break;
                case ThreadState.Suspended:
                    _threadTBGiaHanThe.Resume();
                    break;
            }
        }

        private void btnStopTBGiaHanThe_Click(object sender, EventArgs e)
        {
            _threadTBGiaHanThe.Suspend();

            classExpiredExtensionLogWriter.WriteLog("Stop Service TB Gia Han The");

            btnStopTBGiaHanThe.Enabled = false;
            btnStartTBGiaHanThe.Enabled = true;
        }
        private void btnStartGDTangTien_Click(object sender, EventArgs e)
        {
            if (string.IsNullOrEmpty(txtGDTangTien_BeginDateTime.Text) == false)
            {
                try
                {
                    string updateTime = txtGDTangTien_BeginDateTime.Text;
                    DateTime updateDT = new DateTime();
                    if (DateTime.TryParse(updateTime, out updateDT) == false || updateTime.Length < 23)
                    {
                        MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                        return;
                    }
                    classGDTangTien._updateDateTime = updateTime.Substring(0, 4) + updateTime.Substring(5, 2) + updateTime.Substring(8, 2)
                            + updateTime.Substring(11, 2) + updateTime.Substring(14, 2) + updateTime.Substring(17, 2) + updateTime.Substring(20, 3);
                }
                catch (Exception ex)
                {
                    classGDTangTienLogWriter.WriteLog(ex.Message);
                    return;
                }
                if (MessageBox.Show("Bạn có chắc bắt đầu với thời gian: " +
                        txtGDTangTien_BeginDateTime.Text, "Cảnh báo", MessageBoxButtons.OKCancel) == DialogResult.Cancel)
                    return;
            }
            else
                if (ckbGDTangTien.Checked == true)
                {
                    MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                    return;
                }

            classGDTangTienLogWriter.WriteLog("Start Service GD Tang Tien");

            btnStartGDTangTien.Enabled = false;
            btnStopGDTangTien.Enabled = true;

            switch (_threadGDTangTien.ThreadState)
            {
                case ThreadState.Unstarted:
                    _threadGDTangTien.Start();
                    break;
                case ThreadState.Suspended:
                    _threadGDTangTien.Resume();
                    break;
            }
        }
        private void btnStartGDMCDebit_Click(object sender, EventArgs e)
        {
            if (string.IsNullOrEmpty(txtGDMCDebit_BeginDateTime.Text) == false)
            {
                try
                {
                    string updateTime = txtGDMCDebit_BeginDateTime.Text;
                    DateTime updateDT = new DateTime();
                    if (DateTime.TryParse(updateTime, out updateDT) == false || updateTime.Length < 23)
                    {
                        MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                        return;
                    }
                    classGDMCDebit._updateDateTime = updateTime.Substring(0, 4) + updateTime.Substring(5, 2) + updateTime.Substring(8, 2)
                            + updateTime.Substring(11, 2) + updateTime.Substring(14, 2) + updateTime.Substring(17, 2) + updateTime.Substring(20, 3);
                }
                catch (Exception ex)
                {
                    classGDMCDebitLogWriter.WriteLog(ex.Message);
                    return;
                }
                if (MessageBox.Show("Bạn có chắc bắt đầu với thời gian: " +
                        txtGDMCDebit_BeginDateTime.Text, "Cảnh báo", MessageBoxButtons.OKCancel) == DialogResult.Cancel)
                    return;
            }
            else
                if (ckbGDMCDebit.Checked == true)
                {
                    MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                    return;
                }

            classGDMCDebitLogWriter.WriteLog("Start Service GD MC Debit Doanh Nghiep");

            btnStartGDMCDebit.Enabled = false;
            btnStopGDMCDebit.Enabled = true;

            switch (_threadGDMCDebit.ThreadState)
            {
                case ThreadState.Unstarted:
                    _threadGDMCDebit.Start();
                    break;
                case ThreadState.Suspended:
                    _threadGDMCDebit.Resume();
                    break;
            }

        }
        private void btnStartKichHoatThe_Click(object sender, EventArgs e)
        {
            if (string.IsNullOrEmpty(txtKichHoatThe_BeginDateTime.Text) == false)
            {
                try
                {
                    string updateTime = txtKichHoatThe_BeginDateTime.Text;
                    DateTime updateDT = new DateTime();
                    if (DateTime.TryParse(updateTime, out updateDT) == false || updateTime.Length < 23)
                    {
                        MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                        return;
                    }
                    classKichHoatThe._updateDataTime = updateTime.Substring(0, 4) + updateTime.Substring(5, 2) + updateTime.Substring(8, 2)
                    + updateTime.Substring(11, 2) + updateTime.Substring(14, 2) + updateTime.Substring(17, 2) + updateTime.Substring(20, 3);
                }
                catch (Exception ex)
                {
                    classKichHoatTheLogWriter.WriteLog(ex.Message);
                    return;
                }
                if (MessageBox.Show("Bạn có chắc bắt đầu với thời gian: " +
                    txtKichHoatThe_BeginDateTime.Text, "Cảnh báo", MessageBoxButtons.OKCancel) == DialogResult.Cancel)
                    return;
            }
            else
            {
                if (ckbKichHoatThe.Checked == true)
                {
                    MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                    return;
                }
            }
            classKichHoatTheLogWriter.WriteLog("Start service Kich Hoat The");
            btnStartKichHoatThe.Enabled = false;
            btnStopKichHoatThe.Enabled = true;
            switch (_threadKichHoatThe.ThreadState)
            {
                case ThreadState.Unstarted:
                    _threadKichHoatThe.Start();
                    break;
                case ThreadState.Suspended:
                    _threadKichHoatThe.Resume();
                    break;
            }
        }
        private void btnStartNoQuaHan_Click(object sender, EventArgs e)
        {
            if (string.IsNullOrEmpty(txtNoQuaHan_BeginDateTime.Text) == false)
            {
                try
                {
                    string updateTime = txtNoQuaHan_BeginDateTime.Text;
                    DateTime updateDT = new DateTime();
                    if (DateTime.TryParse(updateTime, out updateDT) == false || updateTime.Length < 23)
                    {
                        MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                        return;
                    }
                    classNoQuaHan._updateDataTime = updateTime.Substring(0, 4) + updateTime.Substring(5, 2) + updateTime.Substring(8, 2)
                            + updateTime.Substring(11, 2) + updateTime.Substring(14, 2) + updateTime.Substring(17, 2) + updateTime.Substring(20, 3);
                }
                catch (Exception ex)
                {
                    classNoQuaHanLogWriter.WriteLog(ex.Message);
                    return;
                }
                if (MessageBox.Show("Bạn có chắc bắt đầu với thời gian: " +
                        txtNoQuaHan_BeginDateTime.Text, "Cảnh báo", MessageBoxButtons.OKCancel) == DialogResult.Cancel)
                    return;
                
            }
            else
                if (ckbNoQuaHan.Checked == true)
                {
                    MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                    return;
                }

            classNoQuaHanLogWriter.WriteLog("Start service No Qua Han at: " + DateTime.Now.ToString());
            btnStartNoQuaHan.Enabled = false;
            btnStopNoQuaHan.Enabled = true;
            switch (_threadNoQuaHan.ThreadState)
            {
                case ThreadState.Unstarted:
                    _threadNoQuaHan.Start();
                    break;
                case ThreadState.Suspended:
                    _threadNoQuaHan.Resume();
                    break;
            }

        }
        private void btnStartGDHoanTra_Click(object sender, EventArgs e)
        {
            if (string.IsNullOrEmpty(txtGDHoanTra_BeginDateTime.Text) == false)
            {
                try
                {
                    string updateTime = txtGDHoanTra_BeginDateTime.Text;
                    DateTime updateDT = new DateTime();
                    if (DateTime.TryParse(updateTime, out updateDT) == false || updateTime.Length < 23)
                    {
                        MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                        return;
                    }
                    classGDHoanTra._updateDateTime = updateTime.Substring(0, 4) + updateTime.Substring(5, 2) + updateTime.Substring(8, 2)
                            + updateTime.Substring(11, 2) + updateTime.Substring(14, 2) + updateTime.Substring(17, 2) + updateTime.Substring(20, 3);
                }
                catch (Exception ex)
                {
                    classGDHoanTraLogWriter.WriteLog(ex.Message);
                    return;
                }
                if (MessageBox.Show("Bạn có chắc bắt đầu với thời gian: " +
                        txtGDHoanTra_BeginDateTime.Text, "Cảnh báo", MessageBoxButtons.OKCancel) == DialogResult.Cancel)
                    return;
            }
            else
                if (ckbGDHoanTra.Checked == true)
                {
                    MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                    return;
                }

            classGDHoanTraLogWriter.WriteLog("Start Service GD Hoan Tra");

            btnStartGDHoanTra.Enabled = false;
            btnStopGDHoanTra.Enabled = true;

            switch (_threadGDHoanTra.ThreadState)
            {
                case ThreadState.Unstarted:
                    _threadGDHoanTra.Start();
                    break;
                case ThreadState.Suspended:
                    _threadGDHoanTra.Resume();
                    break;
            }
        }
        
        private void btnStopGDTangTien_Click(object sender, EventArgs e)
        {
            _threadGDTangTien.Suspend();
            classGDTangTienLogWriter.WriteLog("Stop Service GD Tang Tien");
            btnStartGDTangTien.Enabled = true;
            btnStopGDTangTien.Enabled = false;
        }
        private void btnStopGDHoanTra_Click(object sender, EventArgs e)
        {
            _threadGDHoanTra.Suspend();

            classGDHoanTraLogWriter.WriteLog("Stop Service GD Hoan Tra");

            btnStartGDHoanTra.Enabled = true;
            btnStopGDHoanTra.Enabled = false;
        }
        private void btnStartThuNoFail_Click(object sender, EventArgs e)
        {
            if (string.IsNullOrEmpty(txtThuNoFail_BeginDateTime.Text) == false)
            {
                try
                {
                    classThuNoFailLogWriter.WriteLog("Start Service TB Thu No Fail");
                    string updateTime = txtThuNoFail_BeginDateTime.Text;
                    DateTime updateDT = new DateTime();
                    if (DateTime.TryParse(updateTime, out updateDT) == false || updateTime.Length < 23)
                    {
                        MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                        return;
                    }
                    classThuNoFail._updateDateTime = updateTime.Substring(0, 4) + updateTime.Substring(5, 2) + updateTime.Substring(8, 2)
                            + updateTime.Substring(11, 2) + updateTime.Substring(14, 2) + updateTime.Substring(17, 2) + updateTime.Substring(20, 3);
                }
                catch (Exception ex)
                {
                    classThuNoFailLogWriter.WriteLog(ex.Message);
                    return;
                }
                if (MessageBox.Show("Bạn có chắc bắt đầu với thời gian: " +
                        txtThuNoFail_BeginDateTime.Text, "Cảnh báo", MessageBoxButtons.OKCancel) == DialogResult.Cancel)
                    return;


            }
            else
            {
                if (ckbThuNoFail_.Checked == true)
                {
                    MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                    return;
                }
            }
            btnStartThuNoFail.Enabled = false;
            btnStopThuNoFail.Enabled = true;
            switch (_threadThuNoFail.ThreadState)
            {
                case ThreadState.Unstarted:
                    _threadThuNoFail.Start();
                    break;
                case ThreadState.Suspended:
                    _threadThuNoFail.Resume();
                    break;
            }
            
        }
        private void btnStartThayDoiDuNo_Click(object sender, EventArgs e)
        {
            if (string.IsNullOrEmpty(txtThayDoiDuNo_BeginDateTime.Text) == false)
            {
                try
                {
                    string updateTime = txtThayDoiDuNo_BeginDateTime.Text;
                    DateTime updateDT = new DateTime();
                    if (DateTime.TryParse(updateTime,out updateDT) == false || updateTime.Length < 23)
                    {
                       MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                       return;
                    }
                    classOutstandingChange._updateDateTime = updateTime.Substring(0,4) + updateTime.Substring(5,2) + updateTime.Substring(8,2) 
                            + updateTime.Substring(11,2) + updateTime.Substring(14,2) + updateTime.Substring(17,2) + updateTime.Substring(20,3);
                }
                catch(Exception ex)
                {
                    classOutstandingChangeLogWriter.WriteLog(ex.Message);
                    return;
                }
                if (MessageBox.Show("Bạn có chắc bắt đầu với thời gian: " +
                        txtThayDoiDuNo_BeginDateTime.Text, "Cảnh báo", MessageBoxButtons.OKCancel) == DialogResult.Cancel)
                    return;
            }
            else
                if (ckbThayDoiDuNo_.Checked == true)
                {
                    MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                    return;
                }

            classOutstandingChangeLogWriter.WriteLog("Start Service Thay Doi Du No");

            btnStartThayDoiDuNo.Enabled = false;
            btnStopThayDoiDuNo.Enabled = true;

            switch (_threadThayDoiDuNo.ThreadState)
            {
                case ThreadState.Unstarted:
                    _threadThayDoiDuNo.Start();
                    break;
                case ThreadState.Suspended:
                    _threadThayDoiDuNo.Resume();
                    break;
            }
        }

        private void btnStopThayDoiDuNo_Click(object sender, EventArgs e)
        {
            _threadThayDoiDuNo.Suspend();

            classOutstandingChangeLogWriter.WriteLog("Stop Service Thay Doi Du No");

            btnStopThayDoiDuNo.Enabled = false;
            btnStartThayDoiDuNo.Enabled = true;
        }
   
        private void btnStartNhacNoLan1_Click(object sender, EventArgs e)
        {
            classReminderPayment1LogWriter.WriteLog("Start Service Nhac No Lan 1");
            btnStartNhacNoLan1.Enabled = false;
            btnStopNhacNoLan1.Enabled = true;
            //classReminderPayment1LogWriter.OpenFileWriter();
            switch (_threadNhacNoLan1.ThreadState)
            {
                case ThreadState.Unstarted:
                    _threadNhacNoLan1.Start();
                    break;
                case ThreadState.Suspended:
                    _threadNhacNoLan1.Resume();
                    break;
            }
        }

        private void btnStopNhacNoLan1_Click(object sender, EventArgs e)
        {
            _threadNhacNoLan1.Suspend();

            classReminderPayment1LogWriter.WriteLog("Stop Service Nhac No Lan 1");

            btnStopNhacNoLan1.Enabled = false;
            btnStartNhacNoLan1.Enabled = true;
        }
      
        private void btnStartNhacNoLan2_Click(object sender, EventArgs e)
        {
            classReminderPayment2LogWriter.WriteLog("Start Service Nhac No Lan 2");
            btnStartNhacNoLan2.Enabled = false;
            btnStopNhacNoLan2.Enabled = true;
            //classReminderPayment2LogWriter.OpenFileWriter();
            switch (_threadNhacNoLan2.ThreadState)
            {
                case ThreadState.Unstarted:
                    _threadNhacNoLan2.Start();
                    break;
                case ThreadState.Suspended:
                    _threadNhacNoLan2.Resume();
                    break;
            }
            
        }

        private void btnStopNhacNoLan2_Click(object sender, EventArgs e)
        {
            _threadNhacNoLan2.Suspend();

            classReminderPayment2LogWriter.WriteLog("stop Service Nhac No Lan 2");
            

            btnStopNhacNoLan2.Enabled = false;
            btnStartNhacNoLan2.Enabled = true;
        }

        private void btnStartThanhToanDuNo_TuDong_Click(object sender, EventArgs e)
        {
            if (string.IsNullOrEmpty(txtThanhToanTuDong_BeginDateTime.Text) == false)
            {
                try
                {
                    string updateTime = txtThanhToanTuDong_BeginDateTime.Text;
                    DateTime updateDT = new DateTime();
                    if (DateTime.TryParse(updateTime, out updateDT) == false || updateTime.Length < 23)
                    {
                        MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                        return;
                    }
                    classOutstandingBalancePaymentAuto._updateDateTime = updateTime.Substring(0, 4) + updateTime.Substring(5, 2) + updateTime.Substring(8, 2)
                            + updateTime.Substring(11, 2) + updateTime.Substring(14, 2) + updateTime.Substring(17, 2) + updateTime.Substring(20, 3);
                }
                catch (Exception ex)
                {
                    classOutstandingBalancePaymentAutoLogWriter.WriteLog(ex.Message);
                    return;
                }
                if (MessageBox.Show("Bạn có chắc bắt đầu với thời gian: " +
                        txtThanhToanTuDong_BeginDateTime.Text, "Cảnh báo", MessageBoxButtons.OKCancel) == DialogResult.Cancel)
                    return;
            }
            else
                if (ckbThanhToanTuDong.Checked == true)
                {
                    MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                    return;
                }

            classOutstandingBalancePaymentAutoLogWriter.WriteLog("Start Service Thanh Toan Du No Tu Dong");
            btnStartThanhToanDuNo_TuDong.Enabled = false;
            btnStopThanhToanDuNo_TuDong.Enabled = true;
            
            switch (_threadThanhToanDuNo_TuDong.ThreadState)
            {
                case ThreadState.Unstarted:
                    _threadThanhToanDuNo_TuDong.Start();
                    break;
                case ThreadState.Suspended:
                    _threadThanhToanDuNo_TuDong.Resume();
                    break;
            }
            
        }

        private void btnStopThanhToanDuNo_TuDong_Click(object sender, EventArgs e)
        {
            _threadThanhToanDuNo_TuDong.Suspend();

            classOutstandingBalancePaymentAutoLogWriter.WriteLog("Stop Service Thanh Toan Du No Tu Dong");

            btnStartThanhToanDuNo_TuDong.Enabled = true;
            btnStopThanhToanDuNo_TuDong.Enabled = false;
        }

        private void btnStartThanhToanDuNo_TaiQuay_Click(object sender, EventArgs e)
        {
            if (string.IsNullOrEmpty(txtThanhToanTaiQuay_BeginDateTime.Text) == false)
            {
                try
                {
                    string updateTime = txtThanhToanTaiQuay_BeginDateTime.Text;
                    DateTime updateDT = new DateTime();
                    if (DateTime.TryParse(updateTime, out updateDT) == false || updateTime.Length < 23)
                    {
                        MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                        return;
                    }
                    classOutstandingBalancePaymentManual._updateDateTime = updateTime.Substring(0, 4) + updateTime.Substring(5, 2) + updateTime.Substring(8, 2)
                            + updateTime.Substring(11, 2) + updateTime.Substring(14, 2) + updateTime.Substring(17, 2) + updateTime.Substring(20, 3);
                }
                catch (Exception ex)
                {
                    classOutstandingBalancePaymentManualLogWriter.WriteLog(ex.Message);
                    return;
                }
                if (MessageBox.Show("Bạn có chắc bắt đầu với thời gian: " +
                        txtThanhToanTaiQuay_BeginDateTime.Text, "Cảnh báo", MessageBoxButtons.OKCancel) == DialogResult.Cancel)
                    return;
            }
            else
                if (ckbThanhToanTaiQuay.Checked == true)
                {
                    MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                    return;
                }

            classOutstandingBalancePaymentManualLogWriter.WriteLog("Start Service Thanh Toan Du No Tai Quay");
            btnStartThanhToanDuNo_TaiQuay.Enabled = false;
            btnStopThanhToanDuNo_TaiQuay.Enabled = true;

            switch (_threadThanhToanDuNo_TaiQuay.ThreadState)
            {
                case ThreadState.Unstarted:
                    _threadThanhToanDuNo_TaiQuay.Start();
                    break;
                case ThreadState.Suspended:
                    _threadThanhToanDuNo_TaiQuay.Resume();
                    break;
            }
            
        }

        private void btnStopThanhToanDuNo_TaiQuay_Click(object sender, EventArgs e)
        {
            _threadThanhToanDuNo_TaiQuay.Suspend();

            classOutstandingBalancePaymentManualLogWriter.WriteLog("Stop Service Thanh Toan Du No Tai Quay");

            btnStartThanhToanDuNo_TaiQuay.Enabled = true;
            btnStopThanhToanDuNo_TaiQuay.Enabled = false;
        }

        private void btnStartTBTichLuyDiemThuong_Click(object sender, EventArgs e)
        {
            classAccumulativeRewardPointLogWriter.WriteLog("Start Service TB Diem Thuong");
            btnStartTBTichLuyDiemThuong.Enabled = false;
            btnStopTBTichLuyDiemThuong.Enabled = true;
            //classAccumulativeRewardPointLogWriter.OpenFileWriter();
            switch (_threadTBDiemThuong.ThreadState)
            {
                case ThreadState.Unstarted:
                    _threadTBDiemThuong.Start();
                    break;
                case ThreadState.Suspended:
                    _threadTBDiemThuong.Resume();
                    break;
            }
            
        }

        private void btnStopKichHoatThe_Click(object sender, EventArgs e)
        {
            _threadKichHoatThe.Suspend();
            classKichHoatTheLogWriter.WriteLog("Stop service kich hoat the");
            btnStartKichHoatThe.Enabled = true;
            btnStopKichHoatThe.Enabled = false;
        }

        private void btnStopTBTichLuyDiemThuong_Click(object sender, EventArgs e)
        {
            _threadTBDiemThuong.Suspend();

            classAccumulativeRewardPointLogWriter.WriteLog("Stop Service TB Diem Thuong");

            btnStartTBTichLuyDiemThuong.Enabled = true;
            btnStopTBTichLuyDiemThuong.Enabled = false;
        }

        private void btnExit_Click(object sender, EventArgs e)
        {
            classExpiredExtension.exitThread = true;
            classReminderPayment1.exitThread = true;
            classReminderPayment2.exitThread = true;
            classOutstandingChange._exitThread = true;
            classOutstandingBalancePaymentAuto.exitThread = true;
            classOutstandingBalancePaymentManual.exitThread = true;
            classAccumulativeRewardPoint.exitThread = true;
            classGDHoanTra._exitThread = true;
            classGDTangTien._exitThread = true;
            classKichHoatThe._exitThread = true;
            classCheckDueDate._exitThread = true;
          
            //classExpiredExtensionLogWriter.CloseFileWriter();
            //classReminderPayment1LogWriter.CloseFileWriter();
            //classReminderPayment2LogWriter.CloseFileWriter();
            //classOutstandingChangeLogWriter.CloseFileWriter();
            //classOutstandingBalancePaymentAutoLogWriter.CloseFileWriter();
            //classOutstandingBalancePaymentManualLogWriter.CloseFileWriter();
            //classAccumulativeRewardPointLogWriter.CloseFileWriter();

            //classDataAccessLogWriter.CloseFileWriter();
            this.Close();
        }

        private void ckbThayDoiDuNo_CheckedChanged(object sender, EventArgs e)
        {
            if (ckbThayDoiDuNo_.Checked == true)
            {
                txtThayDoiDuNo_BeginDateTime.Enabled = true;
            }
            else
            {
                txtThayDoiDuNo_BeginDateTime.Text = string.Empty;
                txtThayDoiDuNo_BeginDateTime.Enabled = false;
            }
        }

        private void ckbThanhToanTaiQuay_CheckedChanged(object sender, EventArgs e)
        {
            if (ckbThanhToanTaiQuay.Checked == true)
            {
                txtThanhToanTaiQuay_BeginDateTime.Enabled = true;
            }
            else
            {
                txtThanhToanTaiQuay_BeginDateTime.Text = string.Empty;
                txtThanhToanTaiQuay_BeginDateTime.Enabled = false;
            }
        }

        private void ckbThanhToanTuDong_CheckedChanged(object sender, EventArgs e)
        {
            if (ckbThanhToanTuDong.Checked == true)
            {
                txtThanhToanTuDong_BeginDateTime.Enabled = true;
            }
            else
            {
                txtThanhToanTuDong_BeginDateTime.Text = string.Empty;
                txtThanhToanTuDong_BeginDateTime.Enabled = false;
            }
        }

        

        private void btnStartTBHetHanDoiDiem_Click(object sender, EventArgs e)
        {

        }

        private void btnPauseTBTichLuyDiemThuong_Click(object sender, EventArgs e)
        {

        }

        private void btnResumeTBTichLuyDiemThuong_Click(object sender, EventArgs e)
        {

        }

        private void btnResumeGDHoanTra_Click(object sender, EventArgs e)
        {

        }

        private void btnResumeThayDoiDuNo_Click(object sender, EventArgs e)
        {

        }

        private void btnPauseThayDoiDuNo_Click(object sender, EventArgs e)
        {

        }

        private void btnStopNoQuaHan_Click(object sender, EventArgs e)
        {
            _threadNoQuaHan.Suspend();
            classNoQuaHanLogWriter.WriteLog("Stop service No Qua Han");
            btnStartNoQuaHan.Enabled = true;
            btnStopNoQuaHan.Enabled = false;
        } 

      

        private void btnPauseCheckDueDate_Click(object sender, EventArgs e)
        {

        }

        private void btnStopCheckDueDate_Click(object sender, EventArgs e)
        {
            _threadCheckDueDate.Suspend();
            classCheckDueDateLogWriter.WriteLog("Stop service Check Due Date");
            btnStartCheckDueDate.Enabled = true;
            btnStopCheckDueDate.Enabled = false;
        }

        private void btnStartCheckDueDate_Click(object sender, EventArgs e)
        {
            classCheckDueDateLogWriter.WriteLog("Start Service Check Due Date");
            btnStartCheckDueDate.Enabled = false;
            btnStopCheckDueDate.Enabled = true;
            //classReminderPayment1LogWriter.OpenFileWriter();
            switch (_threadCheckDueDate.ThreadState)
            {
                case ThreadState.Unstarted:
                    _threadCheckDueDate.Start();
                    break;
                case ThreadState.Suspended:
                    _threadCheckDueDate.Resume();
                    break;
            }

        }

        private void btnResumeCheckDueDate_Click(object sender, EventArgs e)
        {

        }
        private void btnStopBlockAndFailAnnualFee_Click(object sender, EventArgs e)
        {
            _threadBlockAndFailFee.Suspend();
            classBlockAndFailAnnualFeeLogWriter.WriteLog("Stop Service Thong Bao Khoa The Va Thu Phi Fail MC DB");
            btnStartBlockAndFailAnnualFee.Enabled = true;
            btnStopBlockAndFailAnnualFee.Enabled = false;

        }
        private void btnStartBlockAndFailAnnualFee_Click(object sender, EventArgs e)
        {


            if (string.IsNullOrEmpty(BlockAndFailAnnualFee_BeginDateTime.Text) == false)
            {
                try
                {
                    string updateTime = BlockAndFailAnnualFee_BeginDateTime.Text;
                    DateTime updateDT = new DateTime();
                    if (DateTime.TryParse(updateTime, out updateDT) == false || updateTime.Length < 23)
                    {
                        MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                        return;
                    }
                    classBlockAndFailAnnualFee._updateDateTime = updateTime.Substring(0, 4) + updateTime.Substring(5, 2) + updateTime.Substring(8, 2)
                            + updateTime.Substring(11, 2) + updateTime.Substring(14, 2) + updateTime.Substring(17, 2) + updateTime.Substring(20, 3);
                }
                catch (Exception ex)
                {
                    classBlockAndFailAnnualFeeLogWriter.WriteLog(ex.Message);
                    return;
                }
                if (MessageBox.Show("Bạn có chắc bắt đầu với thời gian: " +
                        txtPhiThuongNien_BeginDateTime.Text, "Cảnh báo", MessageBoxButtons.OKCancel) == DialogResult.Cancel)
                    return;
            }
            else
                if (ckbPhiThuongNien.Checked == true)
                {
                    MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                    return;
                }

            classBlockAndFailAnnualFeeLogWriter.WriteLog("Start Service Thong Bao Khoa The Va Thu Phi Fail MC DB");

            btnStartBlockAndFailAnnualFee.Enabled = false;
            btnStopBlockAndFailAnnualFee.Enabled = true;

            switch (_threadBlockAndFailFee.ThreadState)
            {
                case ThreadState.Unstarted:
                    _threadBlockAndFailFee.Start();
                    break;
                case ThreadState.Suspended:
                    _threadBlockAndFailFee.Resume();
                    break;
            }
        }
        private void btnStartPhiThuongNien_Click(object sender, EventArgs e)
        {
            if (string.IsNullOrEmpty(txtPhiThuongNien_BeginDateTime.Text) == false)
            {
                try
                {
                    string updateTime = txtPhiThuongNien_BeginDateTime.Text;
                    DateTime updateDT = new DateTime();
                    if (DateTime.TryParse(updateTime, out updateDT) == false || updateTime.Length < 23)
                    {
                        MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                        return;
                    }
                    classAnnual_Fee._updateDateTime = updateTime.Substring(0, 4) + updateTime.Substring(5, 2) + updateTime.Substring(8, 2)
                            + updateTime.Substring(11, 2) + updateTime.Substring(14, 2) + updateTime.Substring(17, 2) + updateTime.Substring(20, 3);
                }
                catch (Exception ex)
                {
                    classAnnual_FeeLogWriter.WriteLog(ex.Message);
                    return;
                }
                if (MessageBox.Show("Bạn có chắc bắt đầu với thời gian: " +
                        txtPhiThuongNien_BeginDateTime.Text, "Cảnh báo", MessageBoxButtons.OKCancel) == DialogResult.Cancel)
                    return;
            }
            else
                if (ckbPhiThuongNien.Checked == true)
                {
                    MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                    return;
                }

            classAnnual_FeeLogWriter.WriteLog("Start Service Thu Phi Thuong Nien");

            btnStartPhiThuongNien.Enabled = false;
            btnStopPhiThuongNien.Enabled = true;

            switch (_threadPhiThuongNien.ThreadState)
            {
                case ThreadState.Unstarted:
                    _threadPhiThuongNien.Start();
                    break;
                case ThreadState.Suspended:
                    _threadPhiThuongNien.Resume();
                    break;
            }
        }

        private void btnPausePhiThuongNien_Click(object sender, EventArgs e)
        {

        }

        private void btnResumePhiThuongNien_Click(object sender, EventArgs e)
        {

        }

        private void btnStopPhiThuongNien_Click(object sender, EventArgs e)
        {
            _threadPhiThuongNien.Suspend();
            classGDTangTienLogWriter.WriteLog("Stop Service Thu Phi Thuog Nien");
            btnStartPhiThuongNien.Enabled = true;
            btnStopPhiThuongNien.Enabled = false;
        }

        private void btnPauseGDTangTien_Click(object sender, EventArgs e)
        {

        }

        private void btnResumeGDTangTien_Click(object sender, EventArgs e)
        {

        }
     

        private void btnStopGDMCDebit_Click(object sender, EventArgs e)
        {
            _threadGDMCDebit.Suspend();
            classGDTangTienLogWriter.WriteLog("Stop Service GD MC Debit");
            btnStartGDMCDebit.Enabled = true;
            btnStopGDMCDebit.Enabled = false;
        }
        
        private void btnPauseGDMCDebit_Click(object sender, EventArgs e)
        {

        }

        private void btnResumeGDMCDebit_Click(object sender, EventArgs e)
        {

        }

        private void btnPauseTBHetHanDoiDiem_Click(object sender, EventArgs e)
        {

        }

        private void label38_Click(object sender, EventArgs e)
        {

        }

        

        private void btnStopThuNoFail_Click(object sender, EventArgs e)
        {
            _threadThuNoFail.Suspend();

            classThuNoFailLogWriter.WriteLog("Stop Service Thu No Fail");

            btnStopThuNoFail.Enabled = false;
            btnStartThuNoFail.Enabled = true;
        }

        private void ckbThuPhiFail_CheckedChanged(object sender, EventArgs e)
        {
            if (ckbThuNoFail_.Checked == true)
            {
                txtThuNoFail_BeginDateTime.Enabled = true;
            }
            else
            {
                txtThuNoFail_BeginDateTime.Text = string.Empty;
                txtThuNoFail_BeginDateTime.Enabled = false;
            }
        }

        private void btnStartDKIPP_Click(object sender, EventArgs e)
        {
            if (string.IsNullOrEmpty(txtDKIPP_BeginDateTime.Text) == false)
            {
                try
                {
                    classDKIPPLogWriter.WriteLog("Start Service TB DK IPP");
                    string updateTime = txtDKIPP_BeginDateTime.Text;
                    DateTime updateDT = new DateTime();
                    if (DateTime.TryParse(updateTime, out updateDT) == false || updateTime.Length < 23)
                    {
                        MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                        return;
                    }
                    classDKIPP._updateDateTime = updateTime.Substring(0, 4) + updateTime.Substring(5, 2) + updateTime.Substring(8, 2)
                            + updateTime.Substring(11, 2) + updateTime.Substring(14, 2) + updateTime.Substring(17, 2) + updateTime.Substring(20, 3);
                }
                catch (Exception ex)
                {
                    classDKIPPLogWriter.WriteLog(ex.Message);
                    return;
                }
                if (MessageBox.Show("Bạn có chắc bắt đầu với thời gian: " +
                        txtDKIPP_BeginDateTime.Text, "Cảnh báo", MessageBoxButtons.OKCancel) == DialogResult.Cancel)
                    return;


            }
            else
            {
                if (ckbDKIPP.Checked == true)
                {
                    MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                    return;
                }
            }
            btnStartDKIPP.Enabled = false;
            btnStopDKIPP.Enabled = true;
            switch (_threadDKIPP.ThreadState)
            {
                case ThreadState.Unstarted:
                    _threadDKIPP.Start();
                    break;
                case ThreadState.Suspended:
                    _threadDKIPP.Resume();
                    break;
            }
        }
        private void btnStopDKIPP_Click(object sender, EventArgs e)
        {
            _threadDKIPP.Suspend();
            classDKIPPLogWriter.WriteLog("Stop Service TB Dang Ki IPP Thanh Cong");
            btnStartDKIPP.Enabled = true;
            btnStopDKIPP.Enabled = false;
        }
        private void btnStartThuNoTatToan_Click(object sender, EventArgs e)
        {
            if (string.IsNullOrEmpty(txtThuNoTatToan_BeginDateTime.Text) == false)
            {
                try
                {
                    classThuNoTatToanIPPLogWriter.WriteLog("Start Service TB Thu No Va Tat Toan IPP");
                    string updateTime = txtThuNoTatToan_BeginDateTime.Text;
                    DateTime updateDT = new DateTime();
                    if (DateTime.TryParse(updateTime, out updateDT) == false || updateTime.Length < 23)
                    {
                        MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                        return;
                    }
                    classThuNoTatToanIPP._updateDateTime = updateTime.Substring(0, 4) + updateTime.Substring(5, 2) + updateTime.Substring(8, 2)
                            + updateTime.Substring(11, 2) + updateTime.Substring(14, 2) + updateTime.Substring(17, 2) + updateTime.Substring(20, 3);
                }
                catch (Exception ex)
                {
                    classThuNoTatToanIPPLogWriter.WriteLog(ex.Message);
                    return;
                }
                if (MessageBox.Show("Bạn có chắc bắt đầu với thời gian: " +
                        txtThuNoTatToan_BeginDateTime.Text, "Cảnh báo", MessageBoxButtons.OKCancel) == DialogResult.Cancel)
                    return;


            }
            else
            {
                if (ckbThuNoTatToan.Checked == true)
                {
                    MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                    return;
                }
            }
            btnStartThuNoTatToan.Enabled = false;
            btnStopThuNoTatToan.Enabled = true;
            switch (_threadThuNoTatToanIPP.ThreadState)
            {
                case ThreadState.Unstarted:
                    _threadThuNoTatToanIPP.Start();
                    break;
                case ThreadState.Suspended:
                    _threadThuNoTatToanIPP.Resume();
                    break;
            }
        }

        private void btnStopThuNoTatToan_Click(object sender, EventArgs e)
        {
            _threadThuNoTatToanIPP.Suspend();
            classThuNoTatToanIPPLogWriter.WriteLog("Stop Service TB Thu No Va Tat Toan IPP");
            btnStartThuNoTatToan.Enabled = true;
            btnStopThuNoTatToan.Enabled = false;
        }

        private void btnStartUpdatePhone_Click(object sender, EventArgs e)
        {
            if (string.IsNullOrEmpty(txtUpdatePhone_BeginDateTime.Text) == false)
            {
                try
                {
                    classUpdatePhoneLogWriter.WriteLog("Start Service Disable Case");
                    string updateTime = txtUpdatePhone_BeginDateTime.Text;
                    DateTime updateDT = new DateTime();
                    if (DateTime.TryParse(updateTime, out updateDT) == false || updateTime.Length < 23)
                    {
                        MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                        return;
                    }
                    classUpdatePhone._updateDateTime = updateTime.Substring(0, 4) + updateTime.Substring(5, 2) + updateTime.Substring(8, 2)
                            + updateTime.Substring(11, 2) + updateTime.Substring(14, 2) + updateTime.Substring(17, 2) + updateTime.Substring(20, 3);
                }
                catch (Exception ex)
                {
                    classUpdatePhoneLogWriter.WriteLog(ex.Message);
                    return;
                }
                if (MessageBox.Show("Bạn có chắc bắt đầu với thời gian: " +
                        txtUpdatePhone_BeginDateTime.Text, "Cảnh báo", MessageBoxButtons.OKCancel) == DialogResult.Cancel)
                    return;


            }
            else
            {
                if (ckbUpdatePhone.Checked == true)
                {
                    MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                    return;
                }
            }
            btnStartUpdatePhone.Enabled = false;
            btnStopUpdatePhone.Enabled = true;
            switch (_threadUpdatePhone.ThreadState)
            {
                case ThreadState.Unstarted:
                    _threadUpdatePhone.Start();
                    break;
                case ThreadState.Suspended:
                    _threadUpdatePhone.Resume();
                    break;
            }
        }

        private void btnPauseUpdatePhone_Click(object sender, EventArgs e)
        {

        }

        private void btnResumeUpdatePhone_Click(object sender, EventArgs e)
        {

        }

        private void btnStopUpdatePhone_Click(object sender, EventArgs e)
        {
            _threadUpdatePhone.Suspend();
            classUpdatePhoneLogWriter.WriteLog("Stop Service Update phone");
            btnStartUpdatePhone.Enabled = true;
            btnStopUpdatePhone.Enabled = false;
        }

        private void btnResumeDKIPP_Click(object sender, EventArgs e)
        {

        }

        private void btnPauseDKIPP_Click(object sender, EventArgs e)
        {

        }

        private void btnPauseThuNoFail_Click(object sender, EventArgs e)
        {

        }

        private void btnStartCPGD_Click(object sender, EventArgs e)
        {
            if (string.IsNullOrEmpty(txtCPGD_BeginDateTime.Text) == false)
            {
                try
                {
                    classCapPhepGDLogWriter.WriteLog("Start Service TB Thu Phi Cap Phep Giao Dich");
                    string updateTime = txtCPGD_BeginDateTime.Text;
                    DateTime updateDT = new DateTime();
                    if (DateTime.TryParse(updateTime, out updateDT) == false || updateTime.Length < 23)
                    {
                        MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                        return;
                    }
                    classCapPhepGD._updateDateTime = updateTime.Substring(0, 4) + updateTime.Substring(5, 2) + updateTime.Substring(8, 2)
                            + updateTime.Substring(11, 2) + updateTime.Substring(14, 2) + updateTime.Substring(17, 2) + updateTime.Substring(20, 3);
                }
                catch (Exception ex)
                {
                    classCapPhepGDLogWriter.WriteLog(ex.Message);
                    return;
                }
                if (MessageBox.Show("Bạn có chắc bắt đầu với thời gian: " +
                        txtCPGD_BeginDateTime.Text, "Cảnh báo", MessageBoxButtons.OKCancel) == DialogResult.Cancel)
                    return;


            }
            else
            {
                if (ckbCPGD.Checked == true)
                {
                    MessageBox.Show("Thời gian bắt đầu không đúng kiểu dữ liệu!");
                    return;
                }
            }
            btnStartCPGD.Enabled = false;
            btnStopCPGD.Enabled = true;
            switch (_threadCPGD.ThreadState)
            {
                case ThreadState.Unstarted:
                    _threadCPGD.Start();
                    break;
                case ThreadState.Suspended:
                    _threadCPGD.Resume();
                    break;
            }
        }

        private void btnStopCPGD_Click(object sender, EventArgs e)
        {
            _threadCPGD.Suspend();
            classCapPhepGDLogWriter.WriteLog("Stop Service TB Thu No Cap Phep Giao Dich");
            btnStartCPGD.Enabled = true;
            btnStopCPGD.Enabled = false;
        }

        private void ckbGDHoanTra_CheckedChanged(object sender, EventArgs e)
        {

        }

        private void BtnPauseTBGiaHanThe_Click(object sender, EventArgs e)
        {

        }

        private void BtnResumeTBGiaHanThe_Click(object sender, EventArgs e)
        {

        }
    }
}