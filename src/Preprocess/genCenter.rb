require 'fileutils'

io=File.open("test","rb")
samp=io.read(136*32)
cnt =0
line=0
samp.each_char{ |t|
	if cnt>8 then
		print " " 
	elsif cnt==0 then 
		print "#{line}\t0\t" 
	end
	if cnt>=8 then  print t.ord.to_s end
	cnt+=1
	if(cnt==136) then 
		puts
		cnt=0
		line+=1
	end
}

#File.open("data","wb"){ |f|
#	f.write(samp)
#}
