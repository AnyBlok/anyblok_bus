# This file is a part of the AnyBlok project
#
#    Copyright (C) 2018 Jean-Sebastien SUZANNE <jssuzanne@anybox.fr>
#    Copyright (C) 2019 Jean-Sebastien SUZANNE <js.suzanne@gmail.com>
#
# This Source Code Form is subject to the terms of the Mozilla Public License,
# v. 2.0. If a copy of the MPL was not distributed with this file,You can
# obtain one at http://mozilla.org/MPL/2.0/.
from anyblok.conftest import *  # noqa
from anyblok.tests.conftest import *  # noqa
from copy import deepcopy
from anyblok.environment import EnvironmentManager
from anyblok.registry import RegistryManager


def reload_registry(registry, function, **kwargs):
    loaded_bloks = deepcopy(RegistryManager.loaded_bloks)
    if function is not None:
        EnvironmentManager.set('current_blok', 'anyblok-test')
        try:
            function(**kwargs)
        finally:
            EnvironmentManager.set('current_blok', None)

    try:
        registry.reload()
    finally:
        RegistryManager.loaded_bloks = loaded_bloks

    return registry
