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
		static int tot = 0,cnt=0;
		static BinaryWriter bw;
		static SIFT sift;
		static KeyPoint[] keypoint;
	
		static void GenSIFT(string path, string filename)
		{
			try
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
						/*	using (new Window("image", rinput))
							{
								Cv2.WaitKey();
							}*/
	
						using (MatOfFloat descriptors = new MatOfFloat())
						{
			
								sift.Run(rinput, null, out keypoint, descriptors);

								cnt += 1;
								Console.Out.WriteLine(descriptors.Rows + " " + cnt);
								var indexer = descriptors.GetIndexer();
								UInt64 v = 0;
								for (int i = 0; i < 16; i++)
								{
									v *= 16;
									if (filename[i] >= '0' && filename[i] <= '9')
									{
										v += (UInt64)filename[i] - '0';
									}
									else
									{
										v += (UInt64)(filename[i] - 'a') + 10;
									}
								}

								for (int i = 0; i < descriptors.Rows; i++)
								{
									bw.Write((UInt64)v);
									for (int j = 0; j < descriptors.Cols; j++)
									{
										Byte b = (Byte)indexer[i, j];
										bw.Write(b);
									}
								}

								tot += descriptors.Rows;

								input.Release();
								rinput.Release();
								descriptors.Release();
							}

					}
				}

			}
			catch
			{
				return;
			}

		}
		static void Main(string[] args)
		{
			if (args.Length<2||args.Length>3)
			{
				Console.Out.WriteLine(args.Length);
				return;
			}

			FileStream fs = new FileStream(args[1], FileMode.Create);
			bw = new BinaryWriter(fs);
			sift = new SIFT();
			
			bool flag=true;
			if (args.Length==3)
			{
				flag = false;
			}

			string[] files = Directory.GetFiles(args[0]);
			foreach (string file in files)
			{
				if (!flag)
				{
					if (Path.GetFileName(file).Equals(args[2]))
					{
						flag = true;
					}
					else
					{
					//	Console.Out.WriteLine(Path.GetFileName(file)+" "+args[2]);
					//	Console.Out.WriteLine(Path.GetFileName(file).Equals(args[2]));
						continue;
					}
				}

				System.Console.Out.WriteLine(file+" "+Path.GetFileName(file));
				GenSIFT(file, Path.GetFileName(file));
			}
			Console.Out.WriteLine(tot);
			Console.Out.WriteLine(cnt);
			
		}
	}
}
