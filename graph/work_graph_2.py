# -*- coding: utf-8 -*-
import codecs
import time
import threading
import multiprocessing
import sys
import logging
import os
import subprocess as cmd
import pickle
import datetime
from typing import Dict

time_format = datetime.datetime.now().strftime('%Y-%m-%d')

class Status(object):
    not_run = 0
    running = 1
    run_success = 2
    run_failed = 3
    name = {0: "not_run", 1: "running", 2: "run_success", 3: "run_failed"}

class WorkNode(object):

    def __init__(self, node_name, description, script_log_path, run_bash_root_path):
        self.node_name = node_name
        self.description = description
        self.next_nodes = set()
        self.previous_nodes = set()
        self.status = multiprocessing.Value('i', Status.not_run)
        self.restart_times = 2
        self.script_log_path = script_log_path
        self.run_bash_root_path = run_bash_root_path
        pass

    def is_run_success(self):
        if self.status.value == Status.run_success:
            return True
        else:
            return False

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

    def add_previous_nodes(self, last_node):
        self.previous_nodes.add(last_node)

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
            t = multiprocessing.Process(target=self.run_script)
            t.start()
        pass

    def __str__(self):
        return f"{self.description}\tstatus: {self.status}\tpre_view_nods: {','.join(self.previous_nodes)}"

    def run_script(self):
        """
        # 脚本运行线程内
        :return:
        """
        log_name = self.get_log_name()
        cmd_ = f"bash {os.path.join(self.run_bash_root_path, self.node_name)} > {self.script_log_path}/{log_name} 2>&1"
        logging.info(f'run {self.node_name} cmd {cmd_}')
        # cmd_ = u"bash " + unicode(self.node_name) + u" > " + unicode(self.script_log_path) + u"/" + unicode(log_name) + u" 2>&1"
        self.status.value = Status.running
        status, _ = cmd.getstatusoutput(cmd_)
        logging.info(f'子线程内 -------------------------; WordNode, run_script运行结束 -> {self.node_name} 结束状态: {status}')
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
            time.sleep(1)
            self.status.value = Status.run_success
            logging.info('子线程内 -------------------------; WordNode, run_script运行成功!! ->' + str(self.node_name))
        else:
            if self.restart_times >= 0:
                logging.warn('子线程内 -------------------------; WordNode, run_script运行失败!! 180后重新运行 ->'
                             + str(self.node_name))
                self.restart_times -= 1
                time.sleep(180)
                logging.warn('子线程内 -------------------------; WordNode, run_script重新运行!! ->' + str(self.node_name))
                self.run_script()
            else:
                self.status.value = Status.run_failed

    def check_self(self):
        """
        1, 自我状态检查, 首页要保证重启次数还有,如果重启次数都用光了, 就不能启动
        2, 运行失败和还未运行的状态才能重新启动, 运行成功和正在运行的状态不能启动
        :return:
        """
        logging.info('主线程 for循环内 check_and_start; WordNode进行自我检查   ->' + str(self.node_name))
        if self.restart_times >= 0:
            if (self.status.value == Status.not_run) or (self.status.value == Status.run_failed):
                logging.info('主线程 for循环内 check_and_start; WordNode进行自我检查:通过 ->' + str(self.node_name))
                return True
            else:
                logging.info('主线程 for循环内 check_and_start; WordNode进行自我检查:失败 ->' + str(self.node_name))
                return False
        else:
            logging.info('主线程 for循环内 check_and_start; WordNode进行自我检查: 重启失败了,工程要退出了 ->' + str(self.node_name))
            return False



class WorkflowGraph(object):

    def __init__(self, config_path, project_path_name, bash_root_path, script_log_path):
        self.config_path = config_path
        self.node_dict: Dict[str, WorkNode] = dict()  # 名字和节点的对应表

        self.finsh_nodes = set()
        self.running_nodes = set()
        self.not_run_nodes = set()

        self.script_log_path = script_log_path
        self.project_path_name = project_path_name
        self.bash_root_path = bash_root_path
        self.is_debug = True
        pass

    def set_script_log_path(self, script_log_path):
        self.script_log_path = script_log_path

    @staticmethod
    def split_node(node_str: str):
        node_list = node_str.strip().split(":")
        return node_list[0].strip(), node_list[1].strip()

    def print_info(self, txt):
        if self.is_debug:
            print(txt)

    def read_config(self):
        with codecs.open(self.config_path, encoding="utf-8") as conf:
            for line in conf:
                self.print_info(f"start process online ------------------------------{line}")
                line = line.strip()
                now_node = line.split("->")[0].strip()
                previous_nodes = line.split("->")[1].strip()

                # process current node
                now_node_name, now_description = WorkflowGraph.split_node(now_node)
                node = WorkNode(now_node_name, now_description,
                                self.script_log_path, self.bash_root_path)
                if now_node_name in self.node_dict:
                    raise Exception(f'config is error {now_node} is repeated')
                self.node_dict[now_node_name] = node
                self.not_run_nodes.add(now_node_name)

                self.print_info(f"current_node_name: {now_node_name}")



                # process previous nodes
                for previous_node in previous_nodes.split(","):
                    previous_node_name, previous_node_description = WorkflowGraph.split_node(previous_node)
                    self.print_info(f"previous_node_name: {previous_node_name}")
                    self.node_dict[now_node_name].add_previous_nodes(previous_node_name)

                    if previous_node_name not in self.node_dict:
                        pre_node = WorkNode(previous_node_name, previous_node_description, self.script_log_path, self.bash_root_path)
                        self.node_dict[previous_node_name] = pre_node


    def print_all_node(self):
        for node in self.node_dict:
            self.print_info("-----------------")
            self.print_info(node)
            self.print_info("前面的节点" + str(self.node_dict[node].previous_nodes))
            self.print_info("后面的节点" + str(self.node_dict[node].next_nodes))

    def check_previous_nodes_status(self, work_node: WorkNode):
        logging.info(f'检查 {work_node.node_name} 节点前置节点状态: {work_node.previous_nodes}')
        previous_node_names = work_node.previous_nodes
        previous_finish = True

        all_previous_status = []
        for previous_node_name in previous_node_names:
            previous_node_status = self.node_dict[previous_node_name].status
            all_previous_status.append(f'{previous_node_name}:\t{Status.name[previous_node_status.value]}')
            if previous_node_status.value != Status.run_success:
                previous_finish = False
        logging.info(f'检查结果: previous_finish: {previous_finish} \n')
        return previous_finish

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
        previous_all_finish = self.check_previous_nodes_status(work_node)
        logging.info('主线程 for循环内 check_and_start: 当前节点状态    ->' + Status.name[work_node.status.value])
        if previous_all_finish:
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

    def check_running_node(self):
        """
        没有任务正在运行?
        """
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

    def start_all_script(self, seconds=2):
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
            logging.error('主线程 for循环内:脚本文件不全')
            return
        logging.info('主线程:工程开始')
        i = 0
        while i <= 3:
            i += 1
            time.sleep(seconds)
            self.print_all_status()

            for node_name, node in self.node_dict.items():

        #         time.sleep(0.1)
                if not node.is_run_success():
                    logging.info(f'主线程 for循环内:开始检查如下节点和状态    ->            {node_name}: {Status.name[node.status.value]}')
                    self.check_and_start(node_name)
        #             logging.info('主线程 for循环内:开始检查如下节点结束 ->' + str(node_name) + "\n\n")
        #
        #     if self.all_success():
        #         for _ in self.node_dict:
        #             logging.info('主线程 for循环内:所有节点运行成功')
        #         f = codecs.open(self.project_path_name + u"." + time_format, mode="wb")
        #         f.write(u"scc")
        #         f.close()
        #         break

    def print_all_status(self):
        x = []
        logging.info('主线程 for循环内:一次循环检查开始, 打印所有节点的状态  ------------>')
        for key in self.node_dict:
            node = self.node_dict[key]
            logging.info(str(key) + ":" + str(Status.name[node.status.value]))
        logging.info('主线程 for循环内:一次循环检查开始, 打印所有节点的状态  ------------>  结束' + '\n')

    def check_file_exist(self):
        """
        检查所有的脚本文件是否存在
        :return:
        """
        file_exist = True
        import os
        for node_path in self.node_dict:
            if not os.path.exists(os.path.join(self.bash_root_path, node_path)):
                logging.warning(f'check: {os.path.join(self.bash_root_path, node_path)}')
                file_exist = False
        return file_exist


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        filename='/home/freeberty/project/PyWorkFlow/graph/test_config/logging.txt',
                        filemode='w',
                        format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

    wfg = WorkflowGraph(config_path='test_config/work_flow_config.conf', project_path_name='test_project',
                        bash_root_path='/home/freeberty/project/PyWorkFlow/graph/test_config',
                        script_log_path='/home/freeberty/project/PyWorkFlow/graph/log')
    wfg.read_config()
    wfg.start_all_script()
