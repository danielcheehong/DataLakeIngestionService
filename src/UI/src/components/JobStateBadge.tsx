import { Badge } from '@/components/ui/badge'

interface Props {
  state: string
}

export function JobStateBadge({ state }: Props) {
  switch (state) {
    case 'Normal':
      return <Badge variant="success">Normal</Badge>
    case 'Paused':
      return <Badge variant="warning">Paused</Badge>
    case 'Complete':
      return <Badge variant="secondary">Complete</Badge>
    case 'Error':
    case 'Blocked':
      return <Badge variant="error">{state}</Badge>
    case 'None':
      return <Badge variant="outline">None</Badge>
    default:
      return <Badge variant="outline">{state}</Badge>
  }
}
