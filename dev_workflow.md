# 🛠️ Ultimate Developer & Automation Workflow

Dit document beschrijft de architectuur, interactiemodellen en workflows van jouw geavanceerde ontwikkelomgeving. Door de naadloze integratie van **Home Assistant via MCP**, een **volwaardige SSH-terminal met root-toegang**, en **GitHub in de terminal**, is dit de ultieme setup voor het ontwikkelen, testen en beheren van slimme huisautomatiseringen en hardware-bridges.

---

## 📐 Systeem Architectuur & Connectiviteit

De onderstaande Mermaid-visualisatie toont hoe de AI-assistent, de lokale terminal, Home Assistant en de externe systemen met elkaar in verbinding staan:

```mermaid
graph TD
    %% Nodes
    subgraph Lokaal ["💻 Lokale Ontwikkelomgeving (macOS)"]
        Agent["🤖 AI Agent (Antigravity)"]
        Cascade["⚙️ Cascade Client"]
        Terminal["📟 macOS Terminal (Zsh)"]
        Git["🐙 Git / GitHub CLI"]
    end

    subgraph Server ["🏠 Home Assistant Host (192.168.50.106)"]
        HA["🏡 Home Assistant Core (v2026.5.2)"]
        Addon["📦 Modbus-MQTT Bridge Add-on"]
        Docker["🐳 Docker Daemon & Host OS"]
    end

    subgraph Cloud ["🌐 Cloud & Externe Gateways"]
        GitHubRemote["🖥️ GitHub (rthepen/...)"]
        Gateway["📟 USR-N580 Modbus Gateway"]
    end

    %% Connections
    Agent <-->|Aangestuurd via| Cascade
    Cascade <-->|Directe API-aanroepen via MCP| HA
    Agent <-->|Voert commando's uit op| Terminal
    Terminal <-->|Codebeheer & Pushes| Git
    Git <-->|Push/Pull HTTPS/SSH| GitHubRemote
    Terminal <-->|Veilige SSH-Sessie (Poort 22222)| Docker
    Docker <-->|Host voor| HA
    Docker <-->|Host voor| Addon
    Addon <-->|RTU-over-TCP Polling| Gateway
    HA <-->|Meldt status & entiteiten| Addon

    %% Styling
    style Agent fill:#5c7cfa,stroke:#3b5bdb,stroke-width:2px,color:#fff
    style HA fill:#2b8a3e,stroke:#2b8a3e,stroke-width:2px,color:#fff
    style Terminal fill:#495057,stroke:#343a40,stroke-width:2px,color:#fff
    style GitHubRemote fill:#181717,stroke:#181717,stroke-width:2px,color:#fff
    style Addon fill:#e67e22,stroke:#d35400,stroke-width:2px,color:#fff
```

---

## 🔄 De Workflow-cirkel (Ontwikkelen & Debuggen)

Wanneer we een nieuwe feature ontwikkelen of een probleem oplossen, doorlopen we de volgende vier stappen:

```
 ┌────────────────────────────────────────────────────────┐
 │ 1. Analyseren & Ontwerpen                              │
 │    - AI leest code en registers lokaal uit             │
 │    - AI controleert HA entiteiten via MCP              │
 └──────────────────────────┬─────────────────────────────┘
                            │
                            ▼
 ┌────────────────────────────────────────────────────────┐
 │ 2. Wijzigen & Updaten                                  │
 │    - AI bewerkt bronbestanden (bijv. bridge.py)         │
 │    - Versieversie ophogen in config.yaml / addon        │
 └──────────────────────────┬─────────────────────────────┘
                            │
                            ▼
 ┌────────────────────────────────────────────────────────┐
 │ 3. Committen & Pushen                                  │
 │    - Git commando's via de lokale terminal             │
 │    - Code direct naar GitHub gepusht                   │
 └──────────────────────────┬─────────────────────────────┘
                            │
                            ▼
 ┌────────────────────────────────────────────────────────┐
 │ 4. Deployen & Verifiëren                               │
 │    - SSH commando naar Home Assistant host (poort 22222)│
 │    - Add-on updaten, herbouwen en logs uitlezen         │
 │    - Real-time HA status controleren via MCP           │
 └────────────────────────────────────────────────────────┘
```

---

## 🛠️ Geautomatiseerde Deployment (`deploy.sh`)

Om het ontwikkelproces zo soepel en foutloos mogelijk te maken, is er een geautomatiseerd deployment-script `deploy.sh` aanwezig in de root van je workspace. Dit script automatiseert de gehele cirkel:

1. **Versiebeheer**: Hoogt automatisch het patch-versienummer op in `config.yaml` zodat Home Assistant ziet dat er een update is.
2. **Synchronisatie**: Kopieert de lokale root-wijzigingen van `bridge.py` en `config.yaml` naar de `modbus_mqtt_bridge/` map.
3. **Staging & Commit**: Voegt alle gewijzigde bestanden toe en committeert ze met een opgegeven of automatisch gegenereerd bericht.
4. **Push**: Pusht de commits direct naar GitHub (`origin main`).
5. **Supervisor Sync**: Logt in via SSH (poort 22222) en verzoekt Home Assistant om de add-on store te herladen (`ha store reload`).
6. **Update & Restart**: Installeert de nieuwste versie en start de add-on opnieuw op.
7. **Logs streamen**: Begint direct met het tailen van de live logs van de add-on op het scherm.

### Het script uitvoeren:

```zsh
# Start de deployment (het script vraagt om een commit-bericht als je dit niet meegeeft)
./deploy.sh

# Of start met een vooraf gedefinieerd commit-bericht
./deploy.sh "feat: implementeer offline-detectie en skip voor Growatt inverters"
```

---

## 🛠️ Handmatige Commando's & Snelkoppelingen (Voor noodgevallen)

Mocht je handmatig specifieke acties willen uitvoeren, dan kun je gebruik maken van onderstaande commando's:

### 1. Git & GitHub Handmatig (Lokale Terminal)
```zsh
# Status controleren
git status

# Handmatig stage en commit
git add .
git commit -m "manual update"
git push origin main
```

### 2. Home Assistant Host Beheer Handmatig (SSH Root-toegang)
De Home Assistant Supervisor draait op een beveiligde poort `22222`. Hiermee hebben we volledige root-toegang tot de supervisor en Docker containers:

```zsh
# Verbinding maken met de Host OS
ssh root@192.168.50.106 -p 22222

# Add-on handmatig herstarten
ssh root@192.168.50.106 -p 22222 "ha addons restart cb8df8a3_modbus_mqtt_bridge"

# Live logs van de Add-on bekijken
ssh root@192.168.50.106 -p 22222 "ha addons logs cb8df8a3_modbus_mqtt_bridge"
```

### 3. Home Assistant Besturing (via MCP)
De AI kan direct acties uitvoeren op je Home Assistant instance zonder de UI te openen:
* **Entiteiten zoeken**: `ha_search_entities(query="Growatt")`
* **Status uitlezen**: `ha_get_state("sensor.warmtepomp_power_status")`
* **Services aanroepen**: `ha_call_service("switch", "turn_on", entity_id="switch.kws_2_relais")`
* **Systeemstatus controleren**: `ha_get_overview(detail_level="minimal")`

---

## 💎 Voordelen van deze Setup
1. **Razendsnel Itereren**: Binnen 30 seconden is code gewijzigd, gepusht, gedeployed op Home Assistant en zijn de live logs gecontroleerd.
2. **Volledige Controle**: Geen black box. Zowel de softwarematige kant (HA API's) als de systeemkant (SSH/Docker) is direct aanstuurbaar.
3. **Automatische Documentatie**: De AI houdt logboeken (`task.md` en `walkthrough.md`) bij, zodat je altijd weet wat er gewijzigd is.
