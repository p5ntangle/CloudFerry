# Copyright (c) 2014 Mirantis Inc.
#
# Licensed under the Apache License, Version 2.0 (the License);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an AS IS BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and#
# limitations under the License.

"""
Package with OpenStack resources export/import utilities.
"""
from migrationlib.os import osCommon
from utils import log_step, get_log, GeneratorPassword, Postman, Templater
from scheduler.builder_wrapper import inspect_func, supertask
import sqlalchemy
from neutronclient.common.exceptions import IpAddressGenerationFailureClient
import time

LOG = get_log(__name__)


class ResourceImporter(osCommon.osCommon):
    """
    Imports various cloud resources (tenants, users, flavors, etc.) from a container
    prepared by ResourceExporter
    """

    def __init__(self, conf, data={}, users_notifications={}):
        self.config = conf['clouds']['destination']
        self.net_conf = conf['networks'] if "networks" in conf else None
        if 'mail' in conf:
            self.postman = Postman(**conf['mail'])
        else:
            self.postman = None
        self.templater = Templater()
        self.generator = GeneratorPassword()
        self.users_notifications = users_notifications
        self.data = data
        self.funcs = []
        super(ResourceImporter, self).__init__(self.config)

    def __send_msg(self, to, subject, msg):
        if self.postman:
            with self.postman as p:
                p.send(to, subject, msg)

    def __render_template(self, name_file, args):
        if self.templater:
            return self.templater.render(name_file, args)
        else:
            return None

    def __generate_password(self):
        if self.generator:
            return self.generator.get_random_password()
        else:
            return None

    def get_tasks(self):
        return self.funcs

    def set_state(self, obj_dict):
        self.users_notifications = obj_dict['users_notifications']

    def get_state(self):
        return {
            'users_notifications': self.users_notifications,
        }

    def convert_to_dict(self):
        res = self.get_state()
        res['_type_class'] = ResourceImporter.__name__
        return res

    def finish(self):
        for f in self.funcs:
            f()
        self.funcs = []
        LOG.info("| Resource migrated")

    def check_bool(self, bool_entry):
        if isinstance(bool_entry, basestring):
            return True if bool_entry.lower() == "true" else False
        else:
            return bool_entry

    @inspect_func
    @supertask
    def upload(self, data=None, **kwargs):
        self.data = data if data else self.data
        self\
            .upload_roles()\
            .upload_tenants()\
            .upload_flavors()\
            .upload_user_passwords()\
            .send_email_notifications()\
            .upload_security_groups()\
            .upload_network_resources()
        return self

    @inspect_func
    @log_step(LOG)
    def upload_roles(self, data=None, **kwargs):
        roles = data['roles'] if data else self.data['roles']
        # do not import a role if one with the same name already exists
        existing_roles = {r.name.lower() for r in self.keystone_client.roles.list()}
        for role in roles:
            if role.name.lower() not in existing_roles:
                self.keystone_client.roles.create(role.name)
        return self

    @inspect_func
    @log_step(LOG)
    def upload_tenants(self, data=None, **kwargs):
        tenants = data['tenants'] if data else self.data['tenants']
        # do not import tenants or users if ones with the same name already exist
        existing_tenants = {t.name: t for t in self.keystone_client.tenants.list()}
        existing_tenants_lower = {t.name.lower(): t for t in self.keystone_client.tenants.list()}
        existing_users = {u.name: u for u in self.keystone_client.users.list()}
        existing_users_lower = {u.name.lower(): u for u in self.keystone_client.users.list()}
        # by this time roles on source and destination should be synchronized
        roles = {r.name: r for r in self.keystone_client.roles.list()}
        self.users_notifications = {}
        for tenant in tenants:
            if not tenant.name.lower() in existing_tenants_lower:
                dest_tenant = self.keystone_client.tenants.create(tenant_name=tenant.name,
                                                                  description=tenant.description,
                                                                  enabled=self.check_bool(tenant.enabled))
            elif not tenant.name in existing_tenants:
                ex_tenant = existing_tenants_lower[tenant.name.lower()]
                dest_tenant = self.keystone_client.tenants.update(ex_tenant.id, tenant_name=tenant.name)
            else:
                dest_tenant = existing_tenants[tenant.name]
            # import users of this tenant that don't exist yet
            for user in tenant.list_users():
                if user.name.lower() not in existing_users_lower:
                    new_password = self.__generate_password()
                    dest_user = self.keystone_client.users.create(name=user.name,
                                                                  password=new_password,
                                                                  email=user.email,
                                                                  tenant_id=dest_tenant.id,
                                                                  enabled=self.check_bool(user.enabled))
                    self.users_notifications[user.name] = {
                        'email': user.email,
                        'password': new_password
                    }
                elif user.name not in existing_users:
                    ex_user = existing_users_lower[user.name.lower()]
                    dest_user = self.keystone_client.users.update(ex_user,
                                                                  name=user.name)
                else:
                    dest_user = existing_users[user.name]
                # import roles of this user within the tenant that are not already assigned
                dest_user_roles_lower = {r.name.lower() for r in dest_user.list_roles(dest_tenant)}
                for role in user.list_roles(tenant):
                    if role.name.lower() not in dest_user_roles_lower:
                        for dest_role in roles:
                            if role.name.lower() == dest_role.lower():
                                dest_tenant.add_user(dest_user, roles[dest_role])
        return self

    @inspect_func
    @log_step(LOG)
    def upload_flavors(self, data=None, **kwargs):
        flavors = data['flavors'] if data else self.data['flavors']
        # do not import a flavor if one with the same name already exists
        existing = {f.name for f in self.nova_client.flavors.list(is_public=None)}
        for (flavor, tenants) in flavors:
            if flavor.name not in existing:
                if flavor.swap == "":
                    flavor.swap = 0
                dest_flavor = self.nova_client.flavors.create(name=flavor.name,
                                                              ram=flavor.ram,
                                                              vcpus=flavor.vcpus,
                                                              disk=flavor.disk,
                                                              swap=flavor.swap,
                                                              rxtx_factor=flavor.rxtx_factor,
                                                              ephemeral=flavor.ephemeral,
                                                              is_public=self.check_bool(flavor.is_public))
                for tenant in tenants:
                    dest_tenant = self.keystone_client.tenants.find(name=tenant)
                    self.nova_client.flavor_access.add_tenant_access(dest_flavor, dest_tenant.id)
        return self

    @inspect_func
    @log_step(LOG)
    def upload_user_passwords(self, data=None, **kwargs):
        users = data['users'] if data else self.data['users']
        # upload user password if the user exists both on source and destination
        if users:
            with sqlalchemy.create_engine(self.keystone_db_conn_url).begin() as connection:
                for user in self.keystone_client.users.list():
                    if user.name in users:
                        connection.execute(sqlalchemy.text("UPDATE user SET password = :password WHERE id = :user_id"),
                                           user_id=user.id,
                                           password=users[user.name])
        return self

    @inspect_func
    @log_step(LOG)
    def send_email_notifications(self, users_notifications=None, template='templates/email.html', **kwargs):
        users_notifications = users_notifications if users_notifications else self.users_notifications
        for name in users_notifications:
            self.__send_msg(users_notifications[name]['email'],
                            'New password notification',
                            self.__render_template(template,
                                                   {'name': name,
                                                    'password': users_notifications[name]['password']}))
        return self

    def __upload_nova_security_groups(self, security_groups):
        existing = {sg.name for sg in self.nova_client.security_groups.list()}
        for security_group in security_groups:
            if security_group.name not in existing:
                dest_security_group = self.nova_client.security_groups.create(name=security_group.name,
                                                                              description=security_group.description)
                for rule in security_group.rules:
                    self.nova_client.security_group_rules.create(parent_group_id=dest_security_group.id,
                                                                 ip_protocol=rule['ip_protocol'],
                                                                 from_port=rule['from_port'],
                                                                 to_port=rule['to_port'],
                                                                 cidr=rule['ip_range']['cidr'])

    def __upload_neutron_security_groups(self, security_groups):
        # existing = {sg['name'] for sg in self.network_client.list_security_groups()['security_groups']}
        existing = {sg.name for sg in self.nova_client.security_groups.list()}
        for security_group in security_groups:
            if security_group['name'] not in existing:
                dest_security_group = self.network_client.create_security_group({"security_group":{"name":security_group['name'],
                                                                                 "description":security_group['description']}})
                for rule in security_group['security_group_rules']:
                    if rule['protocol']:
                        self.network_client.create_security_group_rule({"security_group_rule":{
                                                                        "direction":rule["direction"],
                                                                        "port_range_min":rule["port_range_min"],
                                                                        "ethertype":rule["ethertype"],
                                                                        "port_range_max":rule["port_range_max"],
                                                                        "protocol":rule["protocol"],
                                                                        "remote_ip_prefix": rule['remote_ip_prefix'],
                                                                        "remote_group_id":dest_security_group['security_group']['security_group_rules'][0]['remote_group_id'],
                                                                        "security_group_id":dest_security_group['security_group']['security_group_rules'][0]['security_group_id']}})

    @inspect_func
    @log_step(LOG)
    def upload_security_groups(self, data=None, **kwargs):
        data = data if data else self.data
        security_groups_info = data['network_service_info']
        if security_groups_info['service'] == "nova" and osCommon.osCommon.network_service(self) == "nova":
            self.__upload_nova_security_groups(security_groups_info['security_groups'])
        if security_groups_info['service'] == "neutron" and osCommon.osCommon.network_service(self) == "neutron":
            self.__upload_neutron_security_groups(security_groups_info['security_groups'])
        if security_groups_info['service'] == "nova" and osCommon.osCommon.network_service(self) == "neutron":
            converted_groups = self.__convert_sg_nova_to_neutron(security_groups_info['security_groups'])
            self.__upload_neutron_security_groups(converted_groups)
        return self

    def __convert_sg_nova_to_neutron(self, security_groups):
        converted_groups = []
        for sg in security_groups:
            converted_group={}
            converted_group['name'] = sg.name
            converted_group['description'] = sg.description
            converted_group['security_group_rules']=[]
            for direction in ["egress", "ingress"]:
                for rule in sg.rules:
                    if direction == "ingress": port = rule['from_port']
                    if direction == "egress": port = rule['to_port']
                    if port == -1: port=None
                    cidr = None
                    if rule['ip_range']: cidr = rule['ip_range']['cidr']
                    converted_group['security_group_rules'].append({"direction": direction,
                                                                    "port_range_min": port,
                                                                    "ethertype": "IPv4",
                                                                    "port_range_max": port,
                                                                    "protocol": rule['ip_protocol'],
                                                                    "remote_ip_prefix": cidr})
            converted_groups.append(converted_group)
        return converted_groups

    @inspect_func
    @log_step(LOG)
    def upload_network_resources(self, data=None, **kwargs):
        data = data if data else self.data
        if self.net_conf:
            self.__network_resources_mappings(data['neutron']['networks'])
            self.__network_resources_mappings(data['neutron']['subnets'])
            self.__network_resources_mappings(data['neutron']['routers'])
            self.__network_resources_mappings(data['neutron']['ports'])
        if data['network_service_info']['service'] == 'neutron':
            self.__upload_neutron_networks(data['neutron']['networks'])
            self.__upload_neutron_subnets(data['neutron']['subnets'])
            self.__upload_neutron_routers(data['neutron']['routers'], data['neutron']['ports'])
            self.__allocating_floatingips(data['neutron']['floatingips'])
            self.__recreate_floatingips(data['neutron']['floatingips'])
            self.__delete_redundant_floatingips(data['neutron']['floatingips'])
        elif data['network_service_info']['service'] == 'nova':
            #NOTE(SOM) may want to add something here to check for rules in the config.
            self.__upload_neutron_networks(data['nova']['networks'])
            self.__upload_neutron_subnets(data['nova']['subnets'])
            self.__allocating_floatingips(data['nova']['floatingips'])
            self.__recreate_floatingips(data['nova']['floatingips'])
            self.__delete_redundant_floatingips(data['nova']['floatingips'])
        return self

    def __network_resources_mappings(self, network_resource_info):
        tenant_map = self.net_conf['mappings']['tenants']
        if 'admin' in tenant_map:
            del tenant_map['admin']
        for resource in network_resource_info:
            if 'tenant_name' in resource:
                if resource['tenant_name'] in tenant_map:
                    resource['tenant_name'] = tenant_map[resource['tenant_name']]
            if 'ext_net_tenant_name' in resource:
                if resource['ext_net_tenant_name'] in tenant_map:
                    resource['ext_net_tenant_name'] = tenant_map[resource['ext_net_tenant_name']]

    def __upload_neutron_networks(self, src_nets, **kwargs):
        existing_nets = self.network_client.list_networks()['networks']
        for src_net in src_nets:
            tenant_id = osCommon.osCommon.get_tenant_id_by_name(self.keystone_client, src_net['tenant_name'])
            tenant_ids_with_src_net = \
                [ex_net['tenant_id'] for ex_net in existing_nets if ex_net['name'] == src_net['name']]
            network_info = {'network': {'name': src_net['name'],
                                        'admin_state_up': src_net['admin_state_up'],
                                        'tenant_id': tenant_id,
                                        'shared': src_net['shared']}}
            if src_net['router:external']:
                network_info['network']['router:external'] = src_net['router:external']
                network_info['network']['provider:physical_network'] = src_net['provider:physical_network']
                network_info['network']['provider:network_type'] = src_net['provider:network_type']
                if src_net['provider:network_type'] == 'vlan':
                    network_info['network']['provider:segmentation_id'] = src_net['provider:segmentation_id']
            if (src_net['name'].lower() not in [name['name'].lower() for name in existing_nets]) \
                    or (tenant_id not in tenant_ids_with_src_net):
                self.network_client.create_network(network_info)
        return self

    def __upload_neutron_subnets(self, src_subnets):
        existing_nets = self.network_client.list_networks()['networks']
        existing_subnets = self.network_client.list_subnets()['subnets']
        for src_subnet in src_subnets:
            tenant_id = osCommon.osCommon.get_tenant_id_by_name(self.keystone_client, src_subnet['tenant_name'])
            tenant_ids_with_src_subnet = \
                [ex_subnet['tenant_id'] for ex_subnet in existing_subnets if ex_subnet['name'] == src_subnet['name']]
            network_id = self.__get_existing_resource_id_by_name(existing_nets, src_subnet['network_name'], tenant_id)
            subnet_info = {'subnet': {'name': src_subnet['name'],
                                      'enable_dhcp': src_subnet['enable_dhcp'],
                                      'network_id': network_id,
                                      'cidr': src_subnet['cidr'],
                                      'allocation_pools': src_subnet['allocation_pools'],
                                      'gateway_ip': src_subnet['gateway_ip'],
                                      'ip_version': src_subnet['ip_version'],
                                      'tenant_id': tenant_id}}
            if (src_subnet['name'].lower() not in [subnet['name'].lower() for subnet in existing_subnets]) \
                    or (tenant_id not in tenant_ids_with_src_subnet):
                self.network_client.create_subnet(subnet_info)
        return self

    def __upload_neutron_routers(self, src_routers, src_router_ports):
        existing_nets = self.network_client.list_networks()['networks']
        existing_routers = self.network_client.list_routers()['routers']
        existing_subnets = self.network_client.list_subnets()['subnets']
        for src_router in src_routers:
            tenant_id = osCommon.osCommon.get_tenant_id_by_name(self.keystone_client, src_router['tenant_name'])
            tenant_ids_with_src_router = \
                [ex_router['tenant_id'] for ex_router in existing_routers if ex_router['name'] == src_router['name']]
            router_info = {'router': {'name': src_router['name'],
                                      'tenant_id': tenant_id}}
            if src_router['external_gateway_info']:
                for ex_net in existing_nets:
                    if ex_net['router:external']:
                        if src_router['ext_net_name'] == ex_net['name']:
                            if src_router['ext_net_tenant_name'] == \
                                    self.keystone_client.tenants.get(ex_net['tenant_id']).name:
                                src_router['external_gateway_info']['network_id'] = ex_net['id']
                                router_info['router']['external_gateway_info'] = src_router['external_gateway_info']
            if (src_router['name'].lower() not in [router['name'].lower() for router in existing_routers]) \
                    or (tenant_id not in tenant_ids_with_src_router):
                router = self.network_client.create_router(router_info)['router']
                for port in filter(lambda rport: rport['tenant_name'] == src_router['tenant_name'], src_router_ports):
                    if port['router_name'] == router['name']:
                        subnet_id = self.__get_existing_resource_id_by_name(existing_subnets, port['subnet_name'],
                                                                            tenant_id)
                        self.network_client.add_interface_router(router['id'],
                                                                 {"subnet_id": subnet_id})
        return self

    def __allocating_floatingips(self, src_floats):
        existing_nets = self.network_client.list_networks()['networks']
        external_nets_ids = []
        # getting list of external networks with allocated floating ips
        for float_src in src_floats:
            public = self.config['import_rules']['overwrite']['public'] if "public" in self.config['import_rules']['overwrite'] else False
            if public:
                extnet_tenant_id = osCommon.osCommon.get_tenant_id_by_name(self.keystone_client,
                                                                           public[float_src['network_name']]['tenant_id'])
                network_id = self.__get_existing_resource_id_by_name(existing_nets,
                                                                public[float_src['network_name']]['dname'], extnet_tenant_id)
            else:
                extnet_tenant_id = osCommon.osCommon.get_tenant_id_by_name(self.keystone_client,
                                                                           float_src['ext_net_tenant_name'])
                network_id = self.__get_existing_resource_id_by_name(existing_nets,
                                                                  float_src['network_name'], extnet_tenant_id)
            if network_id not in external_nets_ids:
                external_nets_ids.append(network_id)
        for external_net_id in external_nets_ids:
            try:
                while True:
                    self.network_client.create_floatingip({'floatingip': {'floating_network_id': external_net_id}})
            except IpAddressGenerationFailureClient:
                LOG.info("| Floating IPs were allocated in network %s" % external_net_id)
        return self

    def __recreate_floatingips(self, src_floats):

        """ We recreate floating ips with the same parameters as on src cloud,
        because we can't determine floating ip address during allocation process. """

        existing_nets = self.network_client.list_networks()['networks']
        existing_floatingips = self.network_client.list_floatingips()['floatingips']
        for float_src in src_floats:
            tenant_id = osCommon.osCommon.get_tenant_id_by_name(self.keystone_client,
                                                                float_src['tenant_name'])
            public = self.config['import_rules']['overwrite']['public'] if "public" in self.config['import_rules']['overwrite'] else False
            if public:
                extnet_tenant_id = osCommon.osCommon.get_tenant_id_by_name(self.keystone_client,
                                                                           public[float_src['network_name']]['tenant_id'])
                extnet_id = self.__get_existing_resource_id_by_name(existing_nets,
                                                                    public[float_src['network_name']]['dname'], extnet_tenant_id)
            else:
                extnet_tenant_id = osCommon.osCommon.get_tenant_id_by_name(self.keystone_client,
                                                                        float_src['ext_net_tenant_name'])
                extnet_id = self.__get_existing_resource_id_by_name(existing_nets,
                                                                   float_src['network_name'], extnet_tenant_id)
            for floating in existing_floatingips:
                if floating['floating_ip_address'] == float_src['floating_ip_address']:
                    if floating['floating_network_id'] == extnet_id:
                        if floating['tenant_id'] != tenant_id:
                            self.network_client.delete_floatingip(floating['id'])
                            self.network_client.create_floatingip({'floatingip': {'floating_network_id': extnet_id,
                                                                                  'tenant_id': tenant_id}})
        return self

    def __delete_redundant_floatingips(self, src_floats):
        existing_floatingips = self.network_client.list_floatingips()['floatingips']
        print "SOM %s" % existing_floatingips
        src_floatingips = [src_float['floating_ip_address'] for src_float in src_floats]
        for floatingip in existing_floatingips:
            if floatingip['floating_ip_address'] not in src_floatingips:
                self.network_client.delete_floatingip(floatingip['id'])
        return self

    def __get_existing_resource_id_by_name(self, existing_resources, src_resource_name, tenant_id):
        for resource in [resource for resource in existing_resources if resource['name'] == src_resource_name]:
            if resource['tenant_id'] == tenant_id:
                return resource['id']
        raise RuntimeError("Can't find suitable resource id with name %s among "
                           "the existing resources %s in tenant = %s" %
                           (src_resource_name, existing_resources, tenant_id))
