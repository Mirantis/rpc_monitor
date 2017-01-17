# ----- SETTINGS ---------

# patch location
PATCH = './mos_patches/mos9.patch'
# destination path for scp
SCP_TO = '/usr/lib/python2.7/dist-packages/'
# do revers patch
REVERS = False

# ----- SETTINGS ---------


from api import *


def main():
    patched = [x.strip() for x in open('./mos_patches/patched_nodes.txt').readlines()]

    # PATCHED NODES PRINT
    cprint(' already patched nodes (./mos_patches/patched_nodes.txt): ')
    for p_node in patched:
        print '--- %s' % p_node

    _cm = fuel_computes()
    _ct = fuel_controllers()
    # FILTER PATCHED NODES
    if not REVERS:
        computes = [node for node in _cm if node not in patched]
        controllers = [node for node in _ct if node not in patched]
        apply_patch(nodes=controllers, patch_file=PATCH, revers=REVERS, location=SCP_TO)
        apply_patch(nodes=computes, patch_file=PATCH, revers=REVERS, location=SCP_TO)
    else:
        apply_patch(nodes=patched, patch_file=PATCH, revers=REVERS, location=SCP_TO)


main()
