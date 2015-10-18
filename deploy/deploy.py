#!/usr/bin/python
import threading
from boto.ec2.autoscale import AutoScaleConnection, Tag
from boto.exception import EC2ResponseError, BotoServerError

import time
from boto.ec2.connection import EC2Connection
from boto.ec2.elb import HealthCheck, ELBConnection
from boto.ec2.autoscale import LaunchConfiguration
from boto.ec2.autoscale import AutoScalingGroup
from boto.ec2.autoscale import ScalingPolicy
from boto.ec2.cloudwatch import MetricAlarm
from boto.ec2.cloudwatch import CloudWatchConnection
from sys import argv


def read_properties(filename):
    properties = []
    for line in open(filename):
        properties.append(line.replace('\n', ''))
    return tuple(properties)


class MSBManager:

    def __init__(self, aws_access_key, aws_secret_key):
        self.ec2_conn = EC2Connection(aws_access_key, aws_secret_key)
        self.elb_conn = ELBConnection(aws_access_key, aws_secret_key)
        self.auto_scale_conn = AutoScaleConnection(aws_access_key, aws_secret_key)
        self.cloud_watch_conn = CloudWatchConnection(aws_access_key, aws_secret_key)
        self.default_cooldown = 60

    def create_security_group(self, name, description):
        sgs = [g for g in self.ec2_conn.get_all_security_groups() if g.name == name]
        sg = sgs[0] if sgs else None
        if not sgs:
            sg = self.ec2_conn.create_security_group(name, description)

        try:
            sg.authorize(ip_protocol="-1", from_port=None, to_port=None, cidr_ip="0.0.0.0/0", dry_run=False)
        except EC2ResponseError:
            pass
        return sg

    def remove_security_group(self, name):
        self.ec2_conn.delete_security_group(name=name)

    def create_instance(self, image, instance_type, key_name, zone, security_groups, tags):
        instance = None
        reservations = self.ec2_conn.get_all_instances()
        for reservation in reservations:
            for i in reservation.instances:
                if 'Name' in i.tags and i.tags['Name'] == tags['Name'] and i.state == 'running':
                    instance = i
                    break

        if not instance:
            reservation = self.ec2_conn.run_instances(image, instance_type=instance_type, key_name=key_name, placement=zone, security_groups=security_groups, monitoring_enabled=True)
            instance = reservation.instances[0]
            while not instance.update() == 'running':
                time.sleep(5)
            time.sleep(10)
            self.ec2_conn.create_tags([instance.id], tags)

        return instance

    def request_spot_instance(self, bid, image, instance_type, key_name, zone, security_groups, tags):
        req = self.ec2_conn.request_spot_instances(price=bid, instance_type=instance_type, image_id=image, availability_zone_group=zone,key_name=key_name, security_groups=security_groups)
        instance_id = None

        while not instance_id:
            job_sir_id = req[0].id
            requests = self.ec2_conn.get_all_spot_instance_requests()
            for sir in requests:
                if sir.id == job_sir_id:
                    instance_id = sir.instance_id
                    break
            time.sleep(60)

        self.ec2_conn.create_tags([instance_id], tags)

    def remove_instance(self, instance_id):
        self.remove_instances([instance_id])

    def remove_instances(self, instance_ids):
        self.ec2_conn.terminate_instances(instance_ids)

    def remove_instance_by_tag_name(self, name):
        reservations = self.ec2_conn.get_all_instances()
        data_centers_intance_ids = []
        for reservation in reservations:
            for instance in reservation.instances:
                if 'Name' in instance.tags and instance.tags['Name'] == name and instance.state == 'running':
                    data_centers_intance_ids.append(instance.id)
        if data_centers_intance_ids:
            self.remove_instances(data_centers_intance_ids)

    def create_elb(self, name, zone, project_tag_value, security_group_id, instance_ids=None):
        lbs = [l for l in self.elb_conn.get_all_load_balancers() if l.name == name]
        lb = lbs[0] if lbs else None
        if not lb:
            hc = HealthCheck(timeout=50, interval=60, healthy_threshold=2, unhealthy_threshold=8, target='HTTP:80/heartbeat')
            ports = [(80, 80, 'http')]
            zones = [zone]
            lb = self.elb_conn.create_load_balancer(name, zones, ports)

            self.elb_conn.apply_security_groups_to_lb(name, [security_group_id])
            lb.configure_health_check(hc)
            if instance_ids:
                lb.register_instances(instance_ids)

            params = {'LoadBalancerNames.member.1': lb.name,
                      'Tags.member.1.Key': '15619project',
                      'Tags.member.1.Value': project_tag_value}
            lb.connection.get_status('AddTags', params, verb='POST')
        return lb

    def remove_elb(self, name):
        self.elb_conn.delete_load_balancer(name)

    def create_launch_configuration(self, name, image, key_name, security_groups, instance_type):
        lcs = [l for l in self.auto_scale_conn.get_all_launch_configurations() if l.name == name]
        lc = lcs[0] if lcs else None
        if not lc:
            lc = LaunchConfiguration(name=name, image_id=image, key_name=key_name,
                                     security_groups=[security_groups], instance_type=instance_type)
            self.auto_scale_conn.create_launch_configuration(lc)
        return lc

    def remove_launch_configuration(self, name):
        self.auto_scale_conn.delete_launch_configuration(name)

    def create_autoscaling_group(self, name, lb_name, zone, tags, instance_ids=None):
        lc = self.create_launch_configuration()
        as_groups = [a for a in self.auto_scale_conn.get_all_groups() if a.name == name]
        as_group = as_groups[0] if as_groups else None
        if not as_group:
            as_group = AutoScalingGroup(group_name=name, load_balancers=[lb_name], availability_zones=[zone],
                                        launch_config=lc, min_size=4, max_size=4, health_check_type='ELB', health_check_period=120, connection=self.auto_scale_conn,
                                        default_cooldown=self.default_cooldown, desired_capacity=4,
                                        tags=tags)

            self.auto_scale_conn.create_auto_scaling_group(as_group)
            if instance_ids:
                self.auto_scale_conn.attach_instances(name, instance_ids)

            scale_up_policy = ScalingPolicy(name='scale_up', adjustment_type='ChangeInCapacity', as_name=name, scaling_adjustment=1, cooldown=self.default_cooldown)
            scale_down_policy = ScalingPolicy(name='scale_down', adjustment_type='ChangeInCapacity', as_name=name, scaling_adjustment=-1, cooldown=self.default_cooldown)

            self.auto_scale_conn.create_scaling_policy(scale_up_policy)
            self.auto_scale_conn.create_scaling_policy(scale_down_policy)

            scale_up_policy = self.auto_scale_conn.get_all_policies(as_group=name, policy_names=['scale_up'])[0]
            scale_down_policy = self.auto_scale_conn.get_all_policies(as_group=name, policy_names=['scale_down'])[0]

            alarm_dimensions = {'AutoScalingGroupName': name}
            scale_up_alarm = MetricAlarm(name='scale_up_on_cpu', namespace='AWS/EC2', metric='CPUUtilization',
                                         statistic='Average', comparison='>', threshold=85, period=60, evaluation_periods=1,
                                         alarm_actions=[scale_up_policy.policy_arn], dimensions=alarm_dimensions)
            self.cloud_watch_conn.create_alarm(scale_up_alarm)
            scale_down_alarm = MetricAlarm(name='scale_down_on_cpu', namespace='AWS/EC2', metric='CPUUtilization', statistic='Average',
                                           comparison='<', threshold=60, period=60, evaluation_periods=1,
                                           alarm_actions=[scale_down_policy.policy_arn], dimensions=alarm_dimensions)
            self.cloud_watch_conn.create_alarm(scale_down_alarm)

        return as_group

    def update_autoscaling_group_max_size(self, as_group, max_size):
        setattr(as_group, 'max_size', max_size)
        as_group.update()

    def update_autoscaling_group_min_size(self, as_group, min_size):
        setattr(as_group, 'min_size', min_size)
        as_group.update()

    def remove_autoscaling_group(self, name):
        self.auto_scale_conn.delete_auto_scaling_group(name)


def request_spot_instance(manager, bid, image, instance_type, key_name, zone, security_groups, tags, instances):
    print 'Requesting spot instance with {} bid, image {} and {}'.format(bid, image, instance_type)
    instances.append(manager.request_spot_instance(bid, image, instance_type, key_name, zone, security_groups, tags))
    print 'Created spot instance with {} bid, image {} and {}'.format(bid, image, instance_type)


def deploy(remove=False):
    aws_access_key, aws_secret_key= read_properties('/home/federico/CMU/Cloud Computing/Amazon/credentials')

    manager = MSBManager(aws_access_key, aws_secret_key)
    region = 'us-east-1'
    zone = 'us-east-1b'
    key_name = 'cloudburstkey'
    ssh_http_sg_name = 'SSH/HTTP'
    phase = 'phase1'

    frontend_image = 'ami-8b3363ee'
    number_of_frontend_servers = 3
    frontend_server_name = 'Frontend Server'
    frontend_elb_name = 'Frontend ELB'
    frontend_servers = []

    if remove:
        manager.remove_instance_by_tag_name(frontend_server_name)
        print 'Frontend Servers removed'
        manager.remove_elb(frontend_elb_name)
        print 'Frontend ELB removed'
    else:
        request_spot_instance_threads = []
        for dummy in xrange(number_of_frontend_servers):
            t = threading.Thread(target=request_spot_instance, args=(manager, 1.0, frontend_image, 'm3.large', key_name, zone, [ssh_http_sg_name], {'Name': frontend_server_name, '15619project': phase}, frontend_servers, ))
            t.start()
            request_spot_instance_threads.append(t)

        for request_spot_instance_thread in request_spot_instance_threads:
            request_spot_instance_thread.join()

        ssh_http_sg = manager.create_security_group(ssh_http_sg_name, ssh_http_sg_name)
        print 'Security Group {} created'.format(ssh_http_sg_name)
        manager.create_elb(frontend_elb_name, zone, phase, ssh_http_sg.id, [frontend_server.id for frontend_server in frontend_servers])
        print 'ELB {} created'.format(frontend_elb_name)

if __name__ == "__main__":
    if argv[1] == 'deploy':
        deploy()
    elif argv[1] == 'remove':
        deploy(True)
    else:
        print 'Invalid option'
    print 'Done'
