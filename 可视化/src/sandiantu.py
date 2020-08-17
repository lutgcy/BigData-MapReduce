#  @data   2019/12/19 21:05
# coding:utf-8
import matplotlib.pyplot as plt
import numpy as np
import pylab as mpl  # import matplotlib as mpl

# 设置汉字格式
# sans-serif就是无衬线字体，是一种通用字体族。
# 常见的无衬线字体有 Trebuchet MS, Tahoma, Verdana, Arial, Helvetica,SimHei 中文的幼圆、隶书等等
mpl.rcParams['font.sans-serif'] = ['FangSong']  # 指定默认字体
mpl.rcParams['axes.unicode_minus'] = False  # 解决保存图像是负号'-'显示为方块的问题

x1, x2, x3, x4, x5, x6 = [], [], [], [], [], []
y1, y2, y3, y4, y5, y6 = [], [], [], [], [], []

for line in open("movies.txt", encoding='utf-8', errors='ignore'):
    items = line.split("\t")
    type_movie = items[2].split("/")[0]
    if type_movie == "剧情":
        x1.append(int(items[5].split("-")[0]))
        y1.append(float(items[1]))
    elif type_movie == "喜剧":
        x2.append(int(items[5].split("-")[0]))
        y2.append(float(items[1]))
    elif type_movie == "科幻":
        x3.append(int(items[5].split("-")[0]))
        y3.append(float(items[1]))
    elif type_movie == "爱情":
        x4.append(int(items[5].split("-")[0]))
        y4.append(float(items[1]))
    elif type_movie == "动作":
        x5.append(int(items[5].split("-")[0]))
        y5.append(float(items[1]))
    else:
        x6.append(int(items[5].split("-")[0]))
        y6.append(float(items[1]))


plt.title(u'散点图')

plt.xlabel('年份')
plt.ylabel('评分')
# # 随机大小
# s = (30*np.random.rand(len(x)))
# # 随机颜色
# c = np.random.rand(len(y))
print(x1, y1)
plt.scatter(x1, y1, marker='o', label="剧情")
plt.scatter(x2, y2, marker='>', label="喜剧")
plt.scatter(x3, y3, marker='<', label="科幻")
plt.scatter(x4, y4, marker='^', label="爱情")
plt.scatter(x5, y5, marker='+', label="动作")
plt.scatter(x6, y6, marker='*', label="其它")

plt.legend(loc='best')
plt.show()



