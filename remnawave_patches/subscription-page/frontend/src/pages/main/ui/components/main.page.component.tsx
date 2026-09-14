/**
 * Замена для: remnawave/subscription-page → frontend/src/pages/main/ui/components/main.page.component.tsx
 *
 * URL и ключ берутся из переменных Vite (префикс VITE_) — их нужно задать при СБОРКЕ фронта, они попадут в bundle.
 *
 * 1) В /opt/remnawave/.env.sub добавьте (без кавычек, без пробелов вокруг =):
 *    VITE_SUB_PAGE_PAY_API_BASE=https://bt.gosocial.run
 *    VITE_SUB_PAGE_PAY_API_KEY=<тот же SUB_PAGE_API_KEY, что в .env бота>
 *
 * 2) Сборка frontend (подставьте путь к клону subscription-page):
 *    docker run --rm -it --env-file /opt/remnawave/.env.sub \
 *      -v /opt/subscription-page/frontend:/work -w /work \
 *      -e NODE_OPTIONS=--max-old-space-size=4096 \
 *      node:24-bookworm-slim bash -lc "npm ci && npm run start:build"
 *
 * 3) docker build образа subscription-page и compose up, как раньше.
 *
 * Почему не просто .env панели: в docker-compose у remnawave-subscription-page
 * обычно только env_file: .env.sub — переменные из .env в этот контейнер не попадают,
 * пока явно не добавите env_file или environment.
 */
import { useCallback, useMemo, useState } from 'react'
import {
    Accordion,
    Box,
    Button,
    Card,
    Center,
    Container,
    Group,
    Image,
    Modal,
    SimpleGrid,
    Stack,
    Text,
    Title
} from '@mantine/core'
import { TSubscriptionPagePlatformKey } from '@remnawave/subscription-page-types'

import {
    AccordionBlockRenderer,
    CardsBlockRenderer,
    InstallationGuideConnector,
    MinimalBlockRenderer,
    RawKeysWidget,
    SubscriptionInfoCardsWidget,
    SubscriptionInfoCollapsedWidget,
    SubscriptionInfoExpandedWidget,
    SubscriptionLinkWidget,
    TimelineBlockRenderer
} from '@widgets/main'
import { useAppConfig, useAppConfigStoreActions, useCurrentLang } from '@entities/app-config-store'
import { useSubscription } from '@entities/subscription-info-store'
import { LanguagePicker } from '@shared/ui/language-picker/language-picker.shared'
import { Page, RemnawaveLogo } from '@shared/ui'

function subPagePayFromBuild(): { apiBase: string; apiKey: string } {
    return {
        apiBase: String(import.meta.env.VITE_SUB_PAGE_PAY_API_BASE ?? '').trim(),
        apiKey: String(import.meta.env.VITE_SUB_PAGE_PAY_API_KEY ?? '').trim()
    }
}

type DurationId = '7' | '30' | '90' | '180' | '365'
type PayMethodId = 'fk_sbp' | 'fk_card' | 'stars' | 'cryptobot'

/** Тарифы, для которых СБП оформляется как рекуррент Platega (см. web_api._create_sbp_checkout). */
const RECURRENT_SBP_DURATIONS: ReadonlySet<DurationId> = new Set([
    '7',
    '30',
    '90',
    '180',
    '365'
])

function sbpRecurrentHint(duration: DurationId): string | null {
    if (!RECURRENT_SBP_DURATIONS.has(duration)) return null
    switch (duration) {
        case '90':
            return 'СБП — автоплатёж: списание каждые 3 месяца по цене тарифа. Отмена в боте (/sub или профиль).'
        case '180':
            return 'СБП — автоплатёж: списание каждые 6 месяцев по цене тарифа. Отмена в боте (/sub или профиль).'
        case '7':
            return 'СБП — автоплатёж с периодическим списанием. Отмена в боте (/sub или профиль).'
        default:
            return 'СБП — автоплатёж с периодическим списанием. Отмена в боте (/sub или профиль).'
    }
}

const PAY_METHODS_ALL: ReadonlyArray<{ id: PayMethodId; label: string }> = [
    { id: 'fk_sbp', label: 'СБП' },
    { id: 'fk_card', label: 'Карты РФ' },
    { id: 'stars', label: 'Telegram Stars' },
    { id: 'cryptobot', label: 'Telegram Cryptobot' }
]
const PAY_METHODS_SITE = PAY_METHODS_ALL.filter(
    (m) => m.id === 'fk_sbp' || m.id === 'fk_card'
)

/**
 * user_id из username страницы подписки.
 * Telegram: 123456789. Сайт: -1833 или n-2 (короткие отрицательные id).
 * Снимаются суффиксы _white, затем _10, затем _3.
 */
function parseSubPageUserId(username: string): number | null {
    let base = username.trim()
    if (base.endsWith('_white')) base = base.slice(0, -'_white'.length)
    if (base.endsWith('_10')) base = base.slice(0, -'_10'.length)
    if (base.endsWith('_3')) base = base.slice(0, -'_3'.length)
    const numeric = (s: string): number | null => {
        if (!/^-?\d+$/.test(s)) return null
        const n = Number.parseInt(s, 10)
        return Number.isFinite(n) ? n : null
    }
    const direct = numeric(base)
    if (direct != null) return direct
    if (base.startsWith('n')) return numeric(base.slice(1))
    return null
}

function SubscriptionPayBlock({ isMobile }: { isMobile: boolean }) {
    const { user } = useSubscription()
    const isWhiteProfile = user.username.includes('_white')
    const userId = useMemo(() => parseSubPageUserId(user.username), [user.username])
    const isSiteUser = userId != null && userId <= 0
    const payMethods = isSiteUser ? PAY_METHODS_SITE : PAY_METHODS_ALL
    const payCfg = useMemo(() => subPagePayFromBuild(), [])
    const subscriptionStillActive = useMemo(() => {
        if (user.userStatus !== 'ACTIVE') return false
        if (user.daysLeft == null) return true
        return Number(user.daysLeft) > 0
    }, [user.daysLeft, user.userStatus])

    const [modalOpen, setModalOpen] = useState(false)
    const [pickedDuration, setPickedDuration] = useState<DurationId | null>(null)
    const [busyMethod, setBusyMethod] = useState<PayMethodId | null>(null)
    const [errorText, setErrorText] = useState<string | null>(null)
    const sbpHint = pickedDuration ? sbpRecurrentHint(pickedDuration) : null

    const openPay = useCallback((d: DurationId) => {
        setErrorText(null)
        setPickedDuration(d)
        setModalOpen(true)
    }, [])

    const closeModal = useCallback(() => {
        if (busyMethod) return
        setModalOpen(false)
        setPickedDuration(null)
        setErrorText(null)
    }, [busyMethod])

    const submitPay = useCallback(
        async (method: PayMethodId) => {
            if (userId == null || pickedDuration == null) return
            if (!payCfg.apiBase || !payCfg.apiKey) return
            setBusyMethod(method)
            setErrorText(null)
            const url = `${payCfg.apiBase.replace(/\/$/, '')}/api/v1/sub_page/pay/${method}`
            try {
                const res = await fetch(url, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-Sub-Page-Api-Key': payCfg.apiKey
                    },
                    body: JSON.stringify({ user_id: userId, duration: pickedDuration })
                })
                const data: unknown = await res.json().catch(() => ({}))
                if (!res.ok) {
                    const msg =
                        typeof data === 'object' &&
                        data !== null &&
                        'detail' in data &&
                        typeof (data as { detail?: unknown }).detail === 'string'
                            ? (data as { detail: string }).detail
                            : `Ошибка ${res.status}`
                    setErrorText(msg)
                    return
                }
                const obj = data as { payment_url?: string; bot_url?: string }
                const redirect = obj.payment_url || obj.bot_url
                if (redirect && typeof redirect === 'string') {
                    window.location.assign(redirect)
                    return
                }
                setErrorText('В ответе нет ссылки для перехода')
            } catch {
                setErrorText('Сеть недоступна или сервер не ответил')
            } finally {
                setBusyMethod(null)
            }
        },
        [pickedDuration, payCfg.apiBase, payCfg.apiKey, userId]
    )

    if (isWhiteProfile) {
        return null
    }

    if (!payCfg.apiBase || !payCfg.apiKey) {
        return (
            <Card p="md" radius="lg" withBorder>
                <Text c="dimmed" size="sm">
                    Оплата: не заданы VITE_SUB_PAGE_PAY_API_BASE и VITE_SUB_PAGE_PAY_API_KEY при сборке
                    фронта. Добавьте их в .env.sub и пересоберите образ (см. комментарий в начале
                    main.page.component.tsx).
                </Text>
            </Card>
        )
    }

    if (userId == null) {
        return (
            <Card p="md" radius="lg" withBorder>
                <Text c="dimmed" size="sm">
                    Оплата: не удалось определить user_id из имени пользователя подписки.
                </Text>
            </Card>
        )
    }

    const tariffBtn = (label: string, duration: DurationId) => (
        <Button
            fullWidth
            justify="space-between"
            onClick={() => openPay(duration)}
            radius="md"
            size={isMobile ? 'sm' : 'md'}
            variant="light"
        >
            <Text fw={500} size="sm" style={{ textAlign: 'left' }}>
                {label}
            </Text>
        </Button>
    )

    return (
        <>
            <Accordion
                chevronPosition="right"
                defaultValue={subscriptionStillActive ? null : 'pay'}
                key={subscriptionStillActive ? 'pay-sub-active' : 'pay-sub-expired'}
                radius="lg"
                variant="separated"
            >
                <Accordion.Item value="pay">
                    <Accordion.Control>
                        <Title c="white" order={5}>
                            Оплата
                        </Title>
                    </Accordion.Control>
                    <Accordion.Panel>
                        <Stack gap="sm">
                            {tariffBtn('Пробный тариф — 7 дней — 99 ₽', '7')}
                            {tariffBtn('1 месяц — 299 ₽', '30')}
                            {tariffBtn('3 месяца — 749 ₽ (СБП — автоплатёж каждые 3 мес.)', '90')}
                            {tariffBtn('6 месяцев — 1349 ₽ (СБП — автоплатёж каждые 6 мес.)', '180')}
                            {tariffBtn('1 год — 2399 ₽ (выгода −33%)', '365')}
                        </Stack>
                    </Accordion.Panel>
                </Accordion.Item>
            </Accordion>

            <Modal
                centered
                onClose={closeModal}
                opened={modalOpen}
                radius="lg"
                title="Выберите способ оплаты"
            >
                <Stack gap="sm">
                    {errorText ? (
                        <Text c="red" size="sm">
                            {errorText}
                        </Text>
                    ) : null}
                    {sbpHint ? (
                        <Text c="dimmed" size="sm">
                            {sbpHint}
                        </Text>
                    ) : null}
                    <SimpleGrid cols={1} spacing="xs">
                        {payMethods.map((m) => (
                            <Button
                                key={m.id}
                                loading={busyMethod === m.id}
                                onClick={() => void submitPay(m.id)}
                                radius="md"
                                variant="filled"
                            >
                                {m.label}
                            </Button>
                        ))}
                    </SimpleGrid>
                    <Button disabled={!!busyMethod} onClick={closeModal} variant="subtle">
                        Отмена
                    </Button>
                </Stack>
            </Modal>
        </>
    )
}

interface IMainPageComponentProps {
    isMobile: boolean
    platform: TSubscriptionPagePlatformKey | undefined
}

const BLOCK_RENDERERS = {
    cards: CardsBlockRenderer,
    timeline: TimelineBlockRenderer,
    accordion: AccordionBlockRenderer,
    minimal: MinimalBlockRenderer
} as const

const SUBSCRIPTION_INFO_BLOCK_RENDERERS = {
    cards: SubscriptionInfoCardsWidget,
    collapsed: SubscriptionInfoCollapsedWidget,
    expanded: SubscriptionInfoExpandedWidget,
    hidden: null
} as const

export const MainPageComponent = ({ isMobile, platform }: IMainPageComponentProps) => {
    const config = useAppConfig()
    const currentLang = useCurrentLang()
    const { setLanguage } = useAppConfigStoreActions()

    const brandName = config.brandingSettings.title
    let hasCustomLogo = !!config.brandingSettings.logoUrl

    if (hasCustomLogo) {
        if (config.brandingSettings.logoUrl.includes('docs.rw')) {
            hasCustomLogo = false
        }
    }

    const hasPlatformApps: Record<TSubscriptionPagePlatformKey, boolean> = {
        ios: Boolean(config.platforms.ios?.apps.length),
        android: Boolean(config.platforms.android?.apps.length),
        linux: Boolean(config.platforms.linux?.apps.length),
        macos: Boolean(config.platforms.macos?.apps.length),
        windows: Boolean(config.platforms.windows?.apps.length),
        androidTV: Boolean(config.platforms.androidTV?.apps.length),
        appleTV: Boolean(config.platforms.appleTV?.apps.length)
    }

    const atLeastOnePlatformApp = Object.values(hasPlatformApps).some((value) => value)

    const SubscriptionInfoBlockRenderer =
        SUBSCRIPTION_INFO_BLOCK_RENDERERS[config.uiConfig.subscriptionInfoBlockType]

    return (
        <Page>
            <Box className="header-wrapper" py="md">
                <Container maw={1200} px={{ base: 'md', sm: 'lg', md: 'xl' }}>
                    <Group justify="space-between">
                        <Group gap="sm" style={{ userSelect: 'none' }} wrap="nowrap">
                            {hasCustomLogo ? (
                                <Image
                                    alt="logo"
                                    fit="contain"
                                    src={config.brandingSettings.logoUrl}
                                    style={{
                                        width: '32px',
                                        height: '32px',
                                        flexShrink: 0
                                    }}
                                />
                            ) : (
                                <RemnawaveLogo c="cyan" size={32} />
                            )}
                            <Title
                                c={hasCustomLogo ? 'white' : 'cyan'}
                                fw={700}
                                order={4}
                                size="lg"
                            >
                                {brandName}
                            </Title>
                        </Group>

                        <SubscriptionLinkWidget
                            hideGetLink={config.baseSettings.hideGetLinkButton}
                            supportUrl={config.brandingSettings.supportUrl}
                        />
                    </Group>
                </Container>
            </Box>

            <Container
                maw={1200}
                px={{ base: 'md', sm: 'lg', md: 'xl' }}
                py="xl"
                style={{ position: 'relative', zIndex: 1 }}
            >
                <Stack gap="xl">
                    {SubscriptionInfoBlockRenderer && (
                        <SubscriptionInfoBlockRenderer isMobile={isMobile} />
                    )}

                    <SubscriptionPayBlock isMobile={isMobile} />

                    {atLeastOnePlatformApp && (
                        <InstallationGuideConnector
                            BlockRenderer={
                                BLOCK_RENDERERS[config.uiConfig.installationGuidesBlockType]
                            }
                            hasPlatformApps={hasPlatformApps}
                            isMobile={isMobile}
                            platform={platform}
                        />
                    )}

                    <RawKeysWidget isMobile={isMobile} />

                    <Center>
                        <LanguagePicker
                            currentLang={currentLang}
                            locales={config.locales}
                            onLanguageChange={setLanguage}
                        />
                    </Center>
                </Stack>
            </Container>
        </Page>
    )
}
