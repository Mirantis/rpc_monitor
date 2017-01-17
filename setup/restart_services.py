from api import *

controllers = fuel_controllers()
computes = fuel_computes()

print 'FUEL CONTROLLERS: ', controllers
print 'FUEL COMPUTES: ', computes

for c in controllers:
    services, resources = split_services_by_crm(c)
    restart_services(c, services)

# for pacemaker enough restart only on one controller
restart_resources(c, resources)

for c in computes:
    services = get_services_list(c)
    restart_services(c, services)
