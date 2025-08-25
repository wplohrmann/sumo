#!/bin/bash

set -e

black sumo
ruff check sumo --fix
