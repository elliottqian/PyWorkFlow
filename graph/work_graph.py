# -*- coding: utf-8 -*-

import codecs
import time
import threading
import sys
import logging
import os

import pickle
import datetime
import commands as cmd

time_format = datetime.datetime.now().strftime('%Y-%m-%d')


class WorkNode(object):

    def __init__(self, script_log_path):
        self.node_name = None
        self.description = None
        self.next_nodes = set()
        self.last = set()
        self.status = 0
        self.restart_times = 2
        self.script_log_path = script_log_path
        pass

    def get_log_name(self):
        """
        生成log文件名称
        :return:
        """
        log_name = self.node_name.split(u"/")[-1].split(u".")[0] + "." + time_format
        return log_name

    def set_restart(self, times=2):
        self.restart_times = times

    def set_name(self, name):
        self.node_name = name

    def add_next(self, next_node):
        self.next_nodes.add(next_node)

    def add_last(self, last_node):
        self.last.add(last_node)

    def set_status(self, status):
        self.status = status

    def start(self):
        """
        # 检查自己的状态, 自己的状态是未运行, 或者运行失败,  才可以运行,  已经运行和运行成功不能运行
        # 将运行结果放到自己的状态, 单线程, 避免多线程出错
        :return:
        """
        if self.check_self():
            logging.info('主线程 for循环内 check_and_start; 子线程内WordNode准备开始运行 ->' + str(self.node_name))
            self.status = Status.running
            t = threading.Thread(target=self.run_script)
            t.start()
        pass

    def run_script(self):
        """
        # 脚本运行线程内
        :return:
        """
        log_name = self.get_log_name()
        cmd_ = u"bash " + unicode(self.node_name) + u" > " \
               + unicode(self.script_log_path) + u"/" + unicode(log_name) + u" 2>&1"
        status, _ = cmd.getstatusoutput(cmd_)
        logging.info('子线程内 -------------------------; WordNode, run_script运行结束 ->' + str(self.node_name)
                     + "结束状态:" + str(status))
        self.check_run_result(status)
        time.sleep(1)

    def check_run_result(self, run_status):
        """
        检查运行结果, 并且切换状态, 保证这段时间 状态数据只能自己访问
        1, 先检查脚本返回状态, 状态=0 运行成功, 其他, 运行失败
        2, 如果运行失败, 就重启吧
        :param run_status:
        :return:
        """
        if run_status == 0:
            self.status = Status.run_success
            logging.info('子线程内 -------------------------; WordNode, run_script运行成功!! ->' + str(self.node_name))
        else:
            if self.restart_times >= 0:
                logging.warn('子线程内 -------------------------; WordNode, run_script运行失败!! 180后重新运行 ->'
                             + str(self.node_name))
                self.restart_times -= 1
                time.sleep(ProjectTime.restart_interval)
                logging.warn('子线程内 -------------------------; WordNode, run_script重新运行!! ->' + str(self.node_name))
                self.run_script()
            else:
                self.status = Status.run_failed

    def check_self(self):
        """
        1, 自我状态检查, 首页要保证重启次数还有,如果重启次数都用光了, 就不能启动
        2, 运行失败和还未运行的状态才能重新启动, 运行成功和正在运行的状态不能启动
        :return:
        """
        logging.info('主线程 for循环内 check_and_start; WordNode进行自我检查   ->' + str(self.node_name))
        if self.restart_times >= 0:
            if (self.status == Status.not_run) or (self.status == Status.run_failed):
                logging.info('主线程 for循环内 check_and_start; WordNode进行自我检查:通过 ->' + str(self.node_name))
                return True
            else:
                logging.info('主线程 for循环内 check_and_start; WordNode进行自我检查:失败 ->' + str(self.node_name))
                return False
        else:
            logging.info('主线程 for循环内 check_and_start; WordNode进行自我检查: 重启失败了,工程要退出了 ->' + str(self.node_name))
            return False


class WorkGraph(object):

    def __init__(self, config_path, project_path_name):
        self.config_path = config_path
        self.node_dict = dict()  # 名字和节点的对应表
        self.script_log_path = None
        self.project_path_name = project_path_name
        pass

    def set_script_log_path(self, script_log_path):
        self.script_log_path = script_log_path

    def check_file_exist(self):
        """
        检查所有的脚本文件是否存在
        :return:
        """
        file_exist = True
        import os
        for node_path in self.node_dict:
            if not os.path.exists(node_path):
                file_exist = False
        return file_exist

    def read_config(self):
        with codecs.open(self.config_path, encoding="utf-8") as conf:
            for line in conf:
                line = line.strip()
                self_node = line.split(u":")[0].strip()
                next_node = line.split(u":")[1].strip()
                self.check_node_dict_next(self_node, next_node)
                self.check_node_dict_last(self_node, next_node)

    def read_config_2(self):
        """
        读取另一种格式的文件
        脚本a:b,c
        表示a依赖于b,c
        """
        with codecs.open(self.config_path, encoding="utf-8") as conf:
            for line in conf:
                line = line.strip()
                if not line.startswith(u"#"):
                    self_nodes = line.split(u":")[1].strip().split(u",")
                    next_node = line.split(u":")[0].strip()
                    for self_node in self_nodes:
                        self_node = self_node.strip()
                        self.check_node_dict_next(self_node, next_node)
                        self.check_node_dict_last(self_node, next_node)

    def check_node_dict_next(self, self_node, next_node):
        if self_node in self.node_dict:
            wn = self.node_dict[self_node]
            wn.add_next(next_node)
        else:
            wn = WorkNode(self.script_log_path)
            wn.set_name(self_node)
            wn.add_next(next_node)
            self.node_dict[self_node] = wn

    def check_node_dict_last(self, self_node, next_node):
        if next_node in self.node_dict:
            wn = self.node_dict[next_node]
            wn.add_last(self_node)
        else:
            wn = WorkNode(self.script_log_path)
            wn.set_name(next_node)
            wn.add_last(self_node)
            self.node_dict[next_node] = wn
        pass

    def print_(self):
        for node in self.node_dict:
            print("-----------------")
            print(node)
            print("前面的节点" + str(self.node_dict[node].last))
            print("后面的节点" + str(self.node_dict[node].next_nodes))
            logging.info("-----------------\n")
            logging.info(str(node))
            logging.info("前面的节点" + str(self.node_dict[node].last))
            logging.info("后面的节点" + str(self.node_dict[node].next_nodes))

    def start_all_script(self, seconds=20):
        """
        整个工程的入口
        1, 先检查所有的脚本文件是否存在, 不存在就返回
        2, 存在就开始循环检查
        3, WorkGraph职责, 负责检查上下文状态, 只要上下文状态满足要求, 就启动节点执行任务
        4, WorkNode职责,  满足上下文状态就执行, 自己的状态自己检查和调整
        5, 当所有节点都是成功状态的时候, 也会退出工程
        :return:
        """
        if not self.check_file_exist():
            logging.warn('主线程 for循环内:脚本文件不全')
            return
        logging.info('主线程:工程开始')
        while True:
            time.sleep(seconds)
            self.print_all_status()
            for node_name in self.node_dict:
                time.sleep(0.1)
                if self.node_dict[node_name].status != Status.run_success:
                    logging.info('主线程 for循环内:开始检查如下节点    ->                                ' + str(node_name))
                    logging.info('主线程 for循环内:当前节点状态       ->' + str(Status.name[self.node_dict[node_name].status]))
                    self.check_and_start(node_name)
                    logging.info('主线程 for循环内:开始检查如下节点结束 ->' + str(node_name) + "\n\n")

            if self.all_success():
                for _ in self.node_dict:
                    logging.info('主线程 for循环内:所有节点运行成功')
                f = codecs.open(self.project_path_name + u"." + time_format, mode="wb")
                f.write(u"scc")
                f.close()
                break

    def all_success(self):
        """
        检查所有节点都是不是成功状态
        :return:
        """
        ok = True
        for w in self.node_dict:
            if self.node_dict[w].status != Status.run_success:
                ok = False
        return ok

    def check_and_start(self, node_name):
        """
        开始运行node的代码入口在这里...........................................
        1, 首先检查前面节点的状态: self.check_last_nodes_status(work_node)
        2, 检查整个工程的状态, 如果有job挂了, 就记录状态, 停止整个工程, 当多次重新运行失败的时候, work_node才会把节点置成失败状态)
        work_node: 当前工作节点
        :param node_name: 当前节点名称
        :return:
        """
        work_node = self.node_dict[node_name]
        last_all_finish = self.check_last_nodes_status(work_node)
        logging.info('主线程 for循环内 check_and_start: 当前节点状态    ->' + Status.name[work_node.status])
        if last_all_finish:
            logging.info('主线程 for循环内 check_and_start: 节点开始运行 ->' + str(work_node.node_name))
            work_node.start()
        else:
            logging.info('主线程 for循环内 check_and_start: 节点等待    ->')

        """如果有job挂了, 并且没有节点是运行状态, 并且重启次数全部用完, 就停止, 这里延时0.1秒等待 work_node.start() 成功改变了状态"""
        time.sleep(1)

        if self.check_failed_job() and self.check_running_node():
            logging.info('主线程 for循环内 check_and_start: 节点等待 准备保存退出->')
            logging.info('主线程 for循环内 check_and_start: 有job挂了, 脚本退出: check_failed_job_success')
            '''
            保存并且停止
            '''
            self.save_status()
            sys.exit(0)

    def check_running_node(self):
        has_no_node_run = True
        for key in self.node_dict:
            node = self.node_dict[key]
            if node.status == Status.running:
                has_no_node_run = False
        return has_no_node_run

    def check_failed_job(self):
        has_failed_job = False
        for key in self.node_dict:
            node = self.node_dict[key]
            if node.status == Status.run_failed and node.restart_times < 0:
                has_failed_job = True
        return has_failed_job

    def check_last_nodes_status(self, work_node):
        logging.info('主线程 for循环内 check_last_nodes_status: 检查前面节点的状态' + str(work_node.node_name))
        last_node_names = work_node.last
        last_finish = True

        for last_node_name in last_node_names:
            last_node_status = self.node_dict[last_node_name].status
            logging.info('主线程 for循环内 check_last_nodes_status: 前面节点的状态:'
                         + str(last_node_name) + ":" + Status.name[last_node_status])
            if last_node_status != Status.run_success:
                last_finish = False
        logging.info(
            '主线程 for循环内 check_last_nodes_status: 前面节点的状态检查结果:' + str(last_finish))
        return last_finish

    def save_status(self, file_path=None):
        """
        将现在的节点状态保存起来
        :param file_path:
        :return:
        """
        if file_path is None:
            file_path = time_format
        output = open(file_path, 'wb')
        pickle.dump(self.node_dict, output)
        output.close()

    def load_status(self, file_path=None):
        """
        载入节点状态
        :param file_path:
        :return:
        """
        if file_path is None:
            file_path = time_format
            logging.info('重启文件地址' + time_format)
            pkl_file = open(file_path, 'rb')
            self.node_dict = pickle.load(pkl_file)
            pkl_file.close()
            logging.info('载入文件结束')

    def print_all_status(self):
        x = []
        for key in self.node_dict:
            node = self.node_dict[key]
            x.append(str(key) + ":")
            x.append(str(Status.name[node.status]) + ", ")
        logging.info('主线程 for循环内:一次循环检查开始, 打印所有节点的状态  ------------>' + '\n' + str(x))

    def restart(self, seconds=200):
        """
        重启脚本
        :param seconds:
        :return:
        """
        # cmd.getstatusoutput("mv " + "log." + time_format + " " + "log." + time_format + ".old")
        self.load_status()
        for key in self.node_dict:
            node = self.node_dict[key]
            node.set_restart()
            # logging.info('重新载入后的状态' + '\n' + str(key) + ":" + Status.name[self.node_dict[key]])
        time.sleep(10)
        logging.info('主线程 重启运行  ------------>' + '\n\n')
        self.start_all_script(seconds)

    def check_hdfs_and_start(self, hdfs_path, check_interval_):
        """
        检查hdfs文件路径, 如果都存在, 才会启动工程, 路径用逗号隔开
        :param hdfs_path:        hdfs 路径, 逗号隔开
        :param check_interval_:  整个工程检查间隔
        """
        i = 0
        while True:
            file_exist = self.check_hdfs(hdfs_path)
            if file_exist:
                break
            time.sleep(check_interval_)
            i += 1
            if i >= 600:
                return

        logging.info('检查hdfs文件均存在 ------------' + '\n\n')
        self.start_all_script(check_interval_)

    @staticmethod
    def check_hdfs(hdfs_path):
        hdfs_paths_ = hdfs_path.strip().split(u",")
        all_file_exist = True
        for hdfs_p in hdfs_paths_:
            hdfs_p = hdfs_p.strip()
            cmd_ = "hdfs dfs -test -e " + hdfs_p
            status, _ = cmd.getstatusoutput(cmd_)
            if status != 0:
                all_file_exist = False
                logging.info(hdfs_p + '   不存在--------------' + '\n')
                break
        return all_file_exist


class Status(object):
    not_run = 0
    running = 1
    run_success = 2
    run_failed = 3
    name = {0: "not_run", 1: "running", 2: "run_success", 3: "run_failed"}


class ProjectTime(object):
    """
    重启间隔
    """
    restart_interval = 300  # 重启间隔


def wait_dependence_project(dependence_projects):
    # 检查文件, 成功跳出, 不成功等待, 1分钟检查一次, 5分钟写日志一次
    l = dependence_projects.split(u";")
    i = 0
    while True:
        i += 1
        if i >= 6:
            i = 0
            logging.info('检查依赖工程中................' + '\n\n')

        success_num = 0
        for name__ in l:
            new_name = name__ + u"." + time_format
            if os.path.isfile(new_name):
                success_num += 1
        if success_num >= len(l):
            logging.info('依赖的工程全部成功' + '\n\n')
            break
        time.sleep(60)
        pass

if __name__ == "__main__":
    work_graph_log_path = sys.argv[4]  # 此处包含工程名称
    project_name = sys.argv[7]

    logging.basicConfig(level=logging.INFO,
                        filename=work_graph_log_path + "/" + project_name + "." + time_format,
                        filemode='w',
                        format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

    """
    参数说明
    1, 配置文件路径
    2, bash脚本日志路径
    3, 启动模式, 目前有3种
    4, py脚本日志路径
    5, 检查间隔
    6, 如果是检查hdfs文件后再启动, 这里存放hdfs路径
    7, 工程名称, 成功之后会创建一个文件
    8, 依赖工程列表, 用分号隔开
    """
    path = sys.argv[1]
    script_log_p = sys.argv[2]
    start_model = sys.argv[3]
    check_interval = int(sys.argv[5])
    dependence_project = sys.argv[8]

    project_p_n = work_graph_log_path + "/" + project_name + "_success"

    """等待依赖工程执行完毕"""
    if dependence_project != u"null":
        wait_dependence_project(dependence_project)

    wg = WorkGraph(path, project_p_n)
    wg.set_script_log_path(script_log_p)

    wg.read_config_2()
    wg.print_()

    if start_model == "start":
        wg.start_all_script(check_interval)
    elif start_model == "restart":
        wg.restart(check_interval)
    elif start_model == "check_hdfs":
        hdfs_paths = sys.argv[6]
        wg.check_hdfs_and_start(hdfs_paths, check_interval)

    pass


"""
还要实现的功能:
1, 检查某hdfs几个文件有没有, 如果存在, 才启动脚本  (已经完成)
2, 打印每个节点的运行时间
3, 重启部分的mv log有bug
4, 增加目录状态, 防止一个工程在读取一个目录的时候  另一个工程来写这个目录  一个 rm -r 工程就挂了啊
   目录状态是全局状态, 多个python脚本应该可以共享
   首先检查目录状态, 写状态, 读状态, 无操作状态
                 写状态下不能读, 读状态下不能写, 写状态下不能写  读状态下可以读   状态名称 数字 用这个来加锁  同时防止死锁, 
                 一次检查一次加载和释放全部可以防止死锁, 或者都按照某个固定的顺序加载可以防止死锁
"""