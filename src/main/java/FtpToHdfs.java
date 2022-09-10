import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class FtpToHdfs {
    //hdfs地址
    private static String _URI = "hdfs://master:9000";
    //hdfs配置对象
    private static Configuration conf = new Configuration();
    //ftp客户端工具对象
    private static FTPClient ftp;

    public static void main(String[] args) throws IOException {
        String ip =  "ftp地址";//ip为要下载文件的远程ftp地址
        String username = "ftp用户名";//远程ftp用户名
        String password = "ftp密码";//远程ftp密码
        String ftpDir = "/home/ftpadmin/UrbanEconomy/";//要下载的数据文件所在ftp文件目录 注意：要全路径,
        String hdfsDir = _URI + "/UrbanEconomy/gather/";//下载到hdfs文件系统路径
        //判断是否存在输出路径，如果存在自动删除
        FileSystem fs = FileSystem.get(conf);
        Path outPutPath=new Path("/UrbanEconomy/");
        if (fs.exists(outPutPath)) {
            fs.delete(outPutPath, true);
        }
        //调用连接ftp方法
        connect(ip,username,password);
        //加载ftp和hdfs文件路径
        loadFromFtpToHdfs(ftpDir,hdfsDir);
        try {
            //ftp登出
            ftp.logout();
            //关闭ftp连接
            ftp.disconnect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 连接ftp的方法
     * @param ip    ftp地址
     * @param username  ftp用户名
     * @param password  ftp密码
     */
    public static void connect(String ip,String username,String password){
        //建立ftp客户端对象
        ftp = new FTPClient();
        try {
            //获取连接
            ftp.connect(ip);
            //获取服务器此次连接返回状态（503则失败）
            int reply = ftp.getReplyCode();
            //通过状态值判断连接是否失败
            if (!FTPReply.isPositiveCompletion(reply)) {
                //连接失败则关闭连接
                ftp.disconnect();
            }
            //客户端用户名密码登陆
            ftp.login(username, password);
            //设置文件类型为二进制文件类型
            ftp.setFileType(FTP.BINARY_FILE_TYPE);
            //设置编码为UTF-8
            ftp.setControlEncoding("UTF-8");
            //设置被动模式，此模式下每次调用连接服务器端会自动开启新端口，防止文件下载不成功
            ftp.enterLocalPassiveMode();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 下载ftp文件的方法
     * @param filePath  远程文件的路径，注意：要系统全路径
     * @param outputPath    下载到hdfs文件路径
     * @return
     */
    public static boolean loadFromFtpToHdfs(String filePath, String outputPath) {
        //设置输入输出流
        InputStream inputStream = null;
        FSDataOutputStream outputStream = null;
        //判断下载是否成功的返回值变量
        boolean flag = true;
        try {
            //切换hdfs文件系统
            FileSystem hdfs = FileSystem.get(conf);
            //获取文件目录下文件装入数组中
            FTPFile[] ftpFiles = ftp.listFiles(filePath);
            int a = 1;
            //便利文件目录下所有文件
            for (FTPFile file:
                 ftpFiles) {
                if(!(file.getName().equals(".") || file.getName().equals(".."))){
                    //下载ftp文件
                    inputStream = ftp.retrieveFileStream(filePath+file.getName());
                    //创建要拷贝到hdfs文件系统路径下的输出流
                    outputStream = hdfs.create(new Path(outputPath+a+File.separator +file.getName()));
                    a++;
                    //通过IOUtils工具类将文件拷贝到hdfs文件系统中
                    IOUtils.copyBytes(inputStream, outputStream, conf, false);
                    //提交ftp命令
                    ftp.completePendingCommand();
                }
            }
        } catch (Exception e) {
            flag = false;
            e.printStackTrace();
        } finally {
            try {
                if (inputStream != null) {
                    //关闭相关的流
                    inputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return flag;
    }


}
