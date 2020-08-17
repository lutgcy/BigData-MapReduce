#  @data   2019/12/20 15:16
import random

import matplotlib.pyplot as plt
import numpy as np
import pylab as mpl  # import matplotlib as mpl

# 设置汉字格式
# sans-serif就是无衬线字体，是一种通用字体族。
# 常见的无衬线字体有 Trebuchet MS, Tahoma, Verdana, Arial, Helvetica,SimHei 中文的幼圆、隶书等等
mpl.rcParams['font.sans-serif'] = ['FangSong']  # 指定默认字体
mpl.rcParams['axes.unicode_minus'] = False  # 解决保存图像是负号'-'显示为方块的问题

actors_fans = []

for line in open("actors.txt", encoding='utf-8', errors='ignore'):
    items = line.replace("\n", "").split("\t")
    actors_fans.append([items[0], int(items[1])])

actors_fans = sorted(actors_fans, key=lambda x: x[1], reverse=True)
actors_fans = actors_fans[:30]
actors_fans.reverse()
print(actors_fans)

actors = [e[0] for e in actors_fans]
fans = [e[1] for e in actors_fans]


fig, ax = plt.subplots()
b = ax.barh(range(len(actors)), fans, color='#6699CC')

# # 设置Y轴纵坐标上的刻度线标签。
ax.set_yticks(range(len(actors)))
ax.set_yticklabels(actors, color='m')

# 不要X横坐标上的label标签。
plt.xticks()

# # 为横向水平的柱图右侧添加数据标签。
for rect in b:
    w = rect.get_width()
    ax.text(w, rect.get_y() + rect.get_height() / 2, '%d' %
            int(w), ha='left', va='center')

plt.title('最受欢迎演员Top30', loc='center', fontsize='14',
          fontweight='bold', color='green')

plt.show()
