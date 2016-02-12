#A similar image search engine based on hadoop.


1. Calculate SIFT vector for each image.
2. Build hierarchical kmeans tree based on SIFT vectors
3. Search the hierarchical kmeans tree and answer queries
 
##Preprocess：

Program.cs *Calculate SIFT vectors*

genCenter.rb *Generate initial cluster centers * 

##Server

**Based on Hadoop 2.7.1**

mapreduce/KMeans.java *KMeans iteration (first loop)*

mapreduce/HKM.java  *Server main program and KMeans iteration(depth>=2)*

mapreduce/BuildIndex.java BuildIndex2.java  *Generate new datas from new cluster centers*

mapreduce/QueryProcessor.java *Handle queries*

netthrow/NetThrow.java *Network service tools*


##Client

**Require OpenCvSharp**

Program.cs Client main program

Client.cs 


##Reference:
Moise, Diana, et al. "Indexing and searching 100M images with Map-Reduce." Proceedings of the 3rd ACM conference on International conference on multimedia retrieval. ACM, 2013.

Muja, Marius, and David G. Lowe. "Fast Approximate Nearest Neighbors with Automatic Algorithm Configuration." VISAPP (1) 2 (2009).

Murphy, William E. Large Scale Hierarchical K-Means Based Image Retrieval With MapReduce. No. AFIT-ENG-14-M-56. AIR FORCE INSTITUTE OF TECHNOLOGY WRIGHT-PATTERSON AFB OH GRADUATE SCHOOL OF ENGINEERING AND MANAGEMENT, 2014.


