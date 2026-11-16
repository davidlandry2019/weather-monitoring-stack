#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Weather monitoring stack installer / updater (Tempest + OpenWeatherMap pollers)
# for Debian/Ubuntu + systemd (VM or LXC container). Same method as
# dashboard-portal and api-interface.
#
# First install (as root):
#   git clone https://github.com/davidlandry2019/weather-monitoring-stack.git /opt/weather-monitoring-stack
#   bash /opt/weather-monitoring-stack/deploy/install.sh
#
# Update later: run the same command. It ALWAYS pulls the latest code first and
# re-runs that fresh copy, so one run is enough. Each run also applies updates
# to python3, git, curl and CA certs, installs requirements.txt, runs the test
# suite and only restarts the pollers if it passes.
#
# Safe to re-run: it never overwrites an existing tempest-poller.env or
# owm-poller.env (except the settings below, when given).
#
# A poller is only (re)started once its env file is filled in (API key/token
# and ClickHouse password no longer "your_..."). Until then any existing poller
# with the same service name - e.g. the old InfluxDB one under /opt/scripts -
# is left running untouched. If a new poller fails its startup checks on its
# first cutover, the old unit is put back and restarted.
#
# Settings (environment variables, all optional):
#   APP_DIR=/opt/weather-monitoring-stack   REPO=davidlandry2019/weather-monitoring-stack
#   BRANCH=<the checked-out branch; master on a first install>
#   SERVICE_USER=weather-poller
#   POLLERS="tempest owm"           which pollers to install
#   REPO_URL=...                    override the clone URL
#   CLICKHOUSE_HOST=...             written into both env files
#   TEMPEST_CLICKHOUSE_PASSWORD=... written into tempest-poller.env
#   OWM_CLICKHOUSE_PASSWORD=...     written into owm-poller.env
#   OLD_DIR=/opt/scripts            where a pre-v2.0 install keeps <poller>/<poller>.env
#   SKIP_TESTS=1                    don't run the test suite before (re)starting
#   SKIP_SYSTEM_UPDATE=1            don't apt-upgrade python3/git/curl/ca-certificates
# ---------------------------------------------------------------------------
set -euo pipefail

APP_DIR=${APP_DIR:-/opt/weather-monitoring-stack}
REPO=${REPO:-davidlandry2019/weather-monitoring-stack}
REPO_URL=${REPO_URL:-https://github.com/$REPO.git}
SERVICE_USER=${SERVICE_USER:-weather-poller}
POLLERS=${POLLERS:-tempest owm}
OLD_DIR=${OLD_DIR:-/opt/scripts}
SYSTEMD_DIR=${SYSTEMD_DIR:-/etc/systemd/system}
PY_MIN=3.9
STARTUP_TIMEOUT=150               # seconds; the pollers retry their startup checks for ~1 min

say()  { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
ok()   { printf '    \033[32m✔\033[0m %s\n' "$*"; }
warn() { printf '    \033[33m!\033[0m %s\n' "$*"; }
die()  { printf '\n\033[1;31mERROR: %s\033[0m\n' "$*" >&2; exit 1; }

# --- preflight -------------------------------------------------------------
[[ $EUID -eq 0 ]] || die "Run as root: sudo bash $0"
command -v systemctl >/dev/null || die "systemd is required."
command -v apt-get >/dev/null || die "This script supports Debian/Ubuntu (apt). Install Python >= $PY_MIN and git by hand, then re-run."

FIRST_INSTALL=1
[[ -d "$APP_DIR/.git" ]] && FIRST_INSTALL=0
if [[ -z "${BRANCH:-}" ]]; then
    BRANCH=master
    if [[ $FIRST_INSTALL -eq 0 ]]; then
        BRANCH=$(git -C "$APP_DIR" symbolic-ref --short -q HEAD 2>/dev/null || echo master)
    fi
fi

# --- 0. self-update: always run the LATEST install.sh ------------------------
# Pull FIRST and re-run the freshly pulled copy, so every step below is the
# newest version. WMS_INSTALL_REEXEC marks the second pass and prevents a loop.
if [[ $FIRST_INSTALL -eq 0 && -z "${WMS_INSTALL_REEXEC:-}" ]] && command -v git >/dev/null; then
    say "Self-update: pulling the latest $BRANCH before anything else"
    if [[ -n "$(git -C "$APP_DIR" status --porcelain --untracked-files=no)" ]]; then
        die "$APP_DIR has local modifications to tracked files. Resolve them (git -C $APP_DIR status) before updating."
    fi
    before=$(git -C "$APP_DIR" rev-parse --short HEAD)
    if git -C "$APP_DIR" fetch -q origin "$BRANCH" && git -C "$APP_DIR" checkout -q "$BRANCH" \
        && git -C "$APP_DIR" merge -q --ff-only "origin/$BRANCH"; then
        after=$(git -C "$APP_DIR" rev-parse --short HEAD)
        if [[ "$before" == "$after" ]]; then ok "already up to date ($after)"; else ok "updated $before -> $after"; fi
    else
        die "Couldn't pull $BRANCH from GitHub (network, or was the branch deleted? Try BRANCH=master). Nothing was changed."
    fi
    export WMS_INSTALL_REEXEC=1 BRANCH
    exec bash "$APP_DIR/deploy/install.sh" "$@"
fi

# --- 1. Python + git ---------------------------------------------------------
say "Python $PY_MIN+ and git"
py_ok() { "$1" -c "import sys, venv, ensurepip; sys.exit(sys.version_info < tuple(map(int, '$PY_MIN'.split('.'))))" 2>/dev/null; }
if ! command -v git >/dev/null || ! command -v curl >/dev/null \
    || ! command -v python3 >/dev/null || ! py_ok python3; then
    apt-get update -qq && apt-get install -y -qq git curl ca-certificates python3 python3-venv >/dev/null
fi
py_ok python3 || die "$(python3 -V 2>&1) is below $PY_MIN (or python3-venv is missing)."
ok "$(python3 -V), $(git --version)"

# --- 1b. keep the runtime patched --------------------------------------------
if [[ "${SKIP_SYSTEM_UPDATE:-0}" != "1" ]]; then
    say "System packages"
    if { apt-get update -qq && apt-get install -y -qq --only-upgrade python3 python3-venv git curl ca-certificates; } \
        > /tmp/weather-monitoring-apt.log 2>&1; then
        ok "apt: python3 python3-venv git curl ca-certificates up to date"
    else
        tail -15 /tmp/weather-monitoring-apt.log
        die "apt upgrade failed (log: /tmp/weather-monitoring-apt.log). Re-run with SKIP_SYSTEM_UPDATE=1 to skip."
    fi
fi

# --- 2. service user ---------------------------------------------------------
say "Service user '$SERVICE_USER'"
if id "$SERVICE_USER" >/dev/null 2>&1; then
    ok "exists"
else
    useradd --system --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
    ok "created"
fi

# --- 3. clone / update -------------------------------------------------------
say "Code in $APP_DIR ($BRANCH)"
if [[ $FIRST_INSTALL -eq 1 ]]; then
    [[ -e "$APP_DIR" && -n "$(ls -A "$APP_DIR" 2>/dev/null)" ]] && die "$APP_DIR exists and isn't a git checkout. Move it aside first."
    git clone -q --branch "$BRANCH" "$REPO_URL" "$APP_DIR"
    ok "cloned ($(git -C "$APP_DIR" log -1 --format='%h %s'))"
    # This run may be an older copy. If the cloned install.sh differs, finish with that one.
    if [[ -z "${WMS_INSTALL_REEXEC:-}" ]] && ! cmp -s "${BASH_SOURCE[0]}" "$APP_DIR/deploy/install.sh"; then
        ok "continuing with the cloned (latest) install.sh"
        export WMS_INSTALL_REEXEC=1 BRANCH
        exec bash "$APP_DIR/deploy/install.sh" "$@"
    fi
else
    ok "at $(git -C "$APP_DIR" log -1 --format='%h %s')"
fi
cd "$APP_DIR"

# --- 4. dependencies + tests -------------------------------------------------
# The virtualenv lives in .venv (git-ignored); it is rebuilt if it was made by
# a Python below PY_MIN or no longer runs (e.g. after a distro upgrade).
say "Dependencies"
if [[ -e .venv ]] && ! py_ok .venv/bin/python; then
    rm -rf .venv
    warn "rebuilt .venv (it was made by an older or missing Python)"
fi
[[ -x .venv/bin/python ]] || python3 -m venv .venv
if ! { .venv/bin/pip install -q --upgrade pip && .venv/bin/pip install -q -r requirements.txt; } \
    > /tmp/weather-monitoring-pip.log 2>&1; then
    tail -20 /tmp/weather-monitoring-pip.log
    die "pip install failed (log: /tmp/weather-monitoring-pip.log). Pollers NOT restarted."
fi
ok "installed requirements.txt"
if [[ "${SKIP_TESTS:-0}" != "1" ]]; then
    say "Test suite"
    if { .venv/bin/pip install -q -r requirements-dev.txt \
        && PYTHONDONTWRITEBYTECODE=1 timeout 300 .venv/bin/python -m pytest -q -p no:cacheprovider tests; } \
        > /tmp/weather-monitoring-tests.log 2>&1; then
        passed=$(grep -oE '[0-9]+ passed' /tmp/weather-monitoring-tests.log | tail -1 || true)
        ok "${passed:-all tests passed}"
    else
        tail -30 /tmp/weather-monitoring-tests.log
        die "Tests failed (full log: /tmp/weather-monitoring-tests.log). Pollers NOT restarted."
    fi
fi
find "$APP_DIR" -name __pycache__ -prune -exec rm -rf {} + 2>/dev/null || true

# --- 5. env files + systemd, one poller at a time ----------------------------
# set_env FILE KEY VALUE: set KEY=VALUE in FILE (any characters in VALUE are safe).
set_env() {
    local file=$1 key=$2
    grep -q "^$key=" "$file" || echo "$key=" >> "$file"
    (umask 077; WMS_VAL=$3 awk -v k="$key" 'index($0, k "=") == 1 { print k "=" ENVIRON["WMS_VAL"]; next } { print }' \
        "$file" > "$file.new")
    cat "$file.new" > "$file" && rm -f "$file.new"
}
get_env() { grep -E "^$2=" "$1" | tail -1 | cut -d= -f2- || true; }

# new_env_from OLD EXAMPLE OUT: the example, with every value the old (pre-v2.0)
# env file had carried over, except INFLUX_*. Old keys the example lacks are appended.
new_env_from() {
    awk -v oldf="$1" '
        FILENAME == oldf {
            if (match($0, /^[A-Za-z_][A-Za-z0-9_]*=/)) {
                k = substr($0, 1, RLENGTH - 1)
                if (k !~ /^INFLUX_/) { if (!(k in old)) order[++n] = k; old[k] = substr($0, RLENGTH + 1) }
            }
            next
        }
        match($0, /^[A-Za-z_][A-Za-z0-9_]*=/) {
            k = substr($0, 1, RLENGTH - 1)
            if (k in old) { print k "=" old[k]; used[k] = 1; next }
        }
        { print }
        END {
            hdr = 0
            for (i = 1; i <= n; i++) if (!(order[i] in used)) {
                if (!hdr) { print ""; print "# Carried over from " oldf; hdr = 1 }
                print order[i] "=" old[order[i]]
            }
        }' "$1" "$2" > "$3"
}

placeholder() { [[ -z "$1" || "$1" == your_* ]]; }

# wait_started SERVICE SINCE: wait until the poller logs that its startup checks passed.
wait_started() {
    local svc=$1 since=$2 log
    for _ in $(seq 1 "$STARTUP_TIMEOUT"); do
        log=$(journalctl -u "$svc" --since "$since" -o cat --no-pager 2>/dev/null || true)
        [[ "$log" == *"Startup checks passed"* ]] && return 0
        if [[ "$log" == *"Startup checks failed"* || "$log" == *"Missing required environment"* \
            || "$log" == *"Could not connect to ClickHouse"* || "$log" == *"No station IDs"* \
            || "$log" == *"No locations configured"* || "$log" == *"CLICKHOUSE_DATABASE:"* ]]; then
            return 1
        fi
        sleep 1
    done
    return 1
}

STARTED=() PENDING=() FAILED=()
for poller in $POLLERS; do
    case $poller in
        tempest) svc=tempest-poller; required="TEMPEST_TOKEN TEMPEST_STATION_IDS CLICKHOUSE_PASSWORD"
                 password_var=TEMPEST_CLICKHOUSE_PASSWORD ;;
        owm)     svc=owm-poller; required="OWM_API_KEY OWM_LOCATIONS CLICKHOUSE_PASSWORD"
                 password_var=OWM_CLICKHOUSE_PASSWORD ;;
        *)       die "Unknown poller '$poller' in POLLERS (use: tempest owm)" ;;
    esac
    env_file="$APP_DIR/$svc.env"
    old_env="$OLD_DIR/$svc/$svc.env"

    say "$svc"
    if [[ -f "$env_file" ]]; then
        ok "$svc.env exists; left unchanged"
    elif [[ -f "$old_env" ]]; then
        new_env_from "$old_env" "$svc.env.example" "$env_file"
        ok "$svc.env created from $svc.env.example, with your settings from $old_env (INFLUX_* dropped)"
    else
        cp "$svc.env.example" "$env_file"
        ok "$svc.env created from $svc.env.example"
    fi
    chown "root:$SERVICE_USER" "$env_file" && chmod 640 "$env_file"

    if [[ -n "${CLICKHOUSE_HOST:-}" ]]; then
        set_env "$env_file" CLICKHOUSE_HOST "$CLICKHOUSE_HOST"; ok "CLICKHOUSE_HOST=$CLICKHOUSE_HOST"
    fi
    if [[ -n "${!password_var:-}" ]]; then
        set_env "$env_file" CLICKHOUSE_PASSWORD "${!password_var}"; ok "CLICKHOUSE_PASSWORD set (from $password_var)"
    elif placeholder "$(get_env "$env_file" CLICKHOUSE_PASSWORD)" && [[ -t 0 ]]; then
        read -r -s -p "    ClickHouse password for $(get_env "$env_file" CLICKHOUSE_USER) (Enter to skip): " pw; echo
        if [[ -n "$pw" ]]; then set_env "$env_file" CLICKHOUSE_PASSWORD "$pw"; ok "CLICKHOUSE_PASSWORD set"; fi
        unset pw
    fi
    chown "root:$SERVICE_USER" "$env_file" && chmod 640 "$env_file"

    missing=()
    for key in $required; do placeholder "$(get_env "$env_file" "$key")" && missing+=("$key"); done
    if [[ ${#missing[@]} -gt 0 ]]; then
        warn "not configured yet: ${missing[*]} in $env_file"
        if systemctl cat "$svc" >/dev/null 2>&1; then
            warn "the existing $svc service was left as it is ($(systemctl is-active "$svc" 2>/dev/null || true))"
        fi
        PENDING+=("$svc")
        continue
    fi
    ok "ClickHouse: $(get_env "$env_file" CLICKHOUSE_USER)@$(get_env "$env_file" CLICKHOUSE_HOST):$(get_env "$env_file" CLICKHOUSE_PORT), database $(get_env "$env_file" CLICKHOUSE_DATABASE)"
    [[ "$(get_env "$env_file" NWS_USER_AGENT)" == *example.com* ]] && warn "NWS_USER_AGENT still has the example contact; set yours in $env_file"

    # Keep the pre-v2.0 (InfluxDB) unit so a failed first cutover can be rolled back.
    unit=$SYSTEMD_DIR/$svc.service
    backup=$unit.pre-v2
    rollback=0
    if [[ -f "$unit" ]] && ! grep -q "$APP_DIR" "$unit"; then
        cp -p "$unit" "$backup"
        rollback=1
        ok "previous unit saved as $backup"
    fi
    sed -e "s|/opt/weather-monitoring-stack|$APP_DIR|g" \
        -e "s|^User=.*|User=$SERVICE_USER|" -e "s|^Group=.*|Group=$SERVICE_USER|" \
        "deploy/$svc.service" > "$unit"
    systemctl daemon-reload
    systemctl enable -q "$svc"
    since=$(date '+%Y-%m-%d %H:%M:%S')
    systemctl restart "$svc"

    if wait_started "$svc" "$since"; then
        ok "running; startup checks passed ($(journalctl -u "$svc" --since "$since" -o cat --no-pager | grep -m1 'Startup checks passed' | sed -e 's/.*reachable (//' -e 's/)$//'))"
        STARTED+=("$svc")
    else
        journalctl -u "$svc" --since "$since" -n 30 --no-pager || true
        if [[ $rollback -eq 1 ]]; then
            cp -p "$backup" "$unit"
            systemctl daemon-reload
            systemctl restart "$svc" || true
            warn "rolled back: the previous $svc unit is running again ($(systemctl is-active "$svc" 2>/dev/null || true))"
        fi
        warn "$svc did not pass its startup checks (logs above). Fix $env_file, then re-run this script."
        FAILED+=("$svc")
    fi
done

# --- done --------------------------------------------------------------------
say "Done: $(git log -1 --format='%h %s')"
[[ ${#STARTED[@]} -gt 0 ]] && ok "running on ClickHouse: ${STARTED[*]}"
[[ ${#PENDING[@]} -gt 0 ]] && warn "waiting for configuration: ${PENDING[*]}"
[[ ${#FAILED[@]} -gt 0 ]] && warn "failed startup checks: ${FAILED[*]}"
cat <<EOF

    Config:  $APP_DIR/tempest-poller.env, $APP_DIR/owm-poller.env
             (then re-run this script, or: systemctl restart tempest-poller owm-poller)
    Logs:    journalctl -u tempest-poller -u owm-poller -f
    Update:  bash $APP_DIR/deploy/install.sh
EOF
if [[ ${#STARTED[@]} -gt 0 && -d "$OLD_DIR" ]]; then
    for svc in "${STARTED[@]}"; do
        [[ -d "$OLD_DIR/$svc" ]] && echo "    Old:     $OLD_DIR/$svc is no longer used; remove it once you're happy (keep its .env until then)."
    done
fi
[[ ${#FAILED[@]} -eq 0 ]] || exit 1
