contents=open("abc").readlines()
for content in contents:
    content = content.split("\t")
    rdomain = content[2].strip()
    url = content[1].strip()
    url = url+chr(10)
    t=open("input/"+rdomain, "a+")
    t.write(url)
    t.close()
