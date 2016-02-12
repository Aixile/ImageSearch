using OpenCvSharp;
using OpenCvSharp.CPlusPlus;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;


namespace SIFT3
{
	class Program
	{
		const int maxSize = 75;
		static SIFT sift;
		static KeyPoint[] keypoint;
		static String tmp_path = "tmp";
		static void GenSIFT(string path,StreamWriter sw )
		{
				using(Mat input = new Mat(path, LoadMode.GrayScale)){
				
					if (input.Empty()) return;
					double h = input.Rows, w = input.Cols;
					double newh, neww;
					if (h > w)
					{
						newh = maxSize;
						neww = Math.Ceiling(w / h * maxSize);
					}
					else
					{
						neww = maxSize;
						newh = Math.Ceiling(h / w * maxSize);
					}
					Size newsize = new Size(neww, newh);
					using (Mat rinput = new Mat())
					{
						Cv2.Resize(input, rinput, newsize);
	
						using (MatOfFloat descriptors = new MatOfFloat())
						{
			
								sift.Run(rinput, null, out keypoint, descriptors);

								var indexer = descriptors.GetIndexer();


								int cnt = 0;
								System.Console.Out.WriteLine(descriptors.Rows);
								for (int i = 0; i < descriptors.Rows; i++)
								{
									String str = i.ToString();
						//		System.Console.Out.WriteLine(cnt);
									str = str + "\t0\t";
									for (int j = 0; j < descriptors.Cols; j++)
									{
										str += indexer[i, j].ToString() + " ";
									}
									sw.Write(str+"\n");
									cnt++;
								}
								input.Release();
								rinput.Release();
								descriptors.Release();
							}
					}
				}
		}
		static void TryShowImage(string path)
		{
			try
			{
				using (Mat input = new Mat(path, LoadMode.Color))
				{
					if (input.Empty()) return;
					using (new Window("path", input))
					{
						Cv2.WaitKey();
					}
				}
			}catch{

			}
		}

		static void Main(string[] args)
		{
			for (int i = 0; i < args.Length; i++) System.Console.Out.WriteLine(args[i]);
			using (StreamWriter sw = new StreamWriter(tmp_path, false))
			{
				sift = new SIFT();
				GenSIFT(args[0],sw);
				sw.Close();
			}

			Client client = new Client("10.141.211.159", 10021);
			client.SendFile(tmp_path);
			string ans = client.Receive();
			System.Console.Out.WriteLine(ans);
			string[] anss = ans.Split(' ');
			for (int i = 0; i < anss.Length; i++)
			{
				string[] t = anss[i].Split(':');
				System.Console.Out.WriteLine(t[0]);
				TryShowImage(@"LibX\"+t[0]);
			}
			client.Close();
		}
	}
}
