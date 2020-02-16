package com.zhiyou100.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class HDFSUtil01 {

	public static void main(String[] args) {

		Configuration conf = new Configuration();
		
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		// 使用 try-with-resource 保证流使用结束后关闭
		try (FileSystem fs = FileSystem.get(conf);){
			
//			Path dwonLoadSrc = new Path("hdfs://master:9000/111.log");
//			Path downLoadDst = new Path("D:/hadoop");
//			
//			fs.copyToLocalFile(dwonLoadSrc, downLoadDst);
//			
//			System.out.println("下载成功");
			
//			Path uploadSrc = new Path("D:/hadoop/222.txt");
//			Path uploadDst = new Path("hdfs://master:9000/aaa");
//			
//			fs.copyFromLocalFile(uploadSrc, uploadDst);
//			
//			System.out.println("上传成功");
			
//			Path dirs = new Path("hdfs://master:9000/bbb/ccc/222");
//			
//			fs.mkdirs(dirs);
//			
//			System.out.println("创建路径上的所有文件夹成功");
			
//			Path deletePath = new Path("hdfs://master:9000/aaa");
//			
//			fs.delete(deletePath, true);
//			
//			System.out.println("文件夹和其中的内容删除成功");
			
			
//			Path renameSrc = new Path("/222.log");
//			Path renameDst = new Path("/bbb/222.log");
//
//			fs.rename(renameSrc, renameDst);
//			
//			System.out.println("重命名文件成功");
			
			
			// 只输出文件, false 只输出当前文件夹下的文件
			// true, 也输出当前文件夹下文件夹中的文件
//			Path listFilePath = new Path("/");
//			
//			RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(listFilePath, true);
//			
//			while (iterator.hasNext()) {
//				
//				LocatedFileStatus lfs = iterator.next();
//				
//				Path lfsPath = lfs.getPath();
//				
//				System.out.println(lfsPath);
//			}
			
			
			// 获取文件夹下的所有内容
//			Path listAllPath = new Path("/");
//			
//			FileStatus[] fileStatus = fs.listStatus(listAllPath);
//			
//			for (FileStatus fss : fileStatus) {
//				
//				Path fssPath = fss.getPath();
//				
//				boolean isdir = fss.isDirectory();
//				
//				System.out.println((isdir ? "* " : "- ") + fssPath);
//			}
			
			// 获取文件夹下的所有内容，包含子文件夹中的内容
//			Path listRootPath = new Path("/");
//			
//			listStatus(fs, listRootPath);
			
			
			Path existsPath = new Path("/bbb/222.log");
			
			if (fs.exists(existsPath)) {
				
				System.out.println(existsPath + " 存在");
			}else {
				
				System.out.println(existsPath + " 不存在");
			}
			
		} catch (IOException e) {

			e.printStackTrace();
		}
	}
	
	
	/**
	 * 展示文件夹下的所有内容
	 * @param path
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static void listStatus(FileSystem fs, Path path) throws FileNotFoundException, IOException {
		
		FileStatus[] fileStatus = fs.listStatus(path);
		
		for (FileStatus fss : fileStatus) {
			
			Path fssPath = fss.getPath();
			
			boolean isDirectory = fss.isDirectory();
			
			System.out.println((isDirectory ? "* " : "- ") + fssPath);
			
			if (isDirectory) {
				
				// 如果是文件夹，就要展示文件夹下的所有内容
				// 调用咱们写好的 listStatus
				listStatus(fs, fssPath);
			}
		}
	}
}






