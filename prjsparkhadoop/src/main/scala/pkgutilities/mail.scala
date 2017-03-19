package pkgutilities

import org.apache.commons.mail._

object mail {

  implicit def stringToSeq(single: String): Seq[String] = Seq(single)
  implicit def liftToOption[T](t: T): Option[T] = Some(t)

  sealed abstract class MailType
  case object Plain extends MailType
  case object Rich extends MailType
  case object MultiPart extends MailType

    def send(
          from: (String, String), // (email -> name)
          to: Seq[String],
          cc: Seq[String] = Seq.empty,
          bcc: Seq[String] = Seq.empty,
          subject: String,
          message: String,
          richMessage: Option[String] = None,
          attachment: Option[(java.io.File)] = None) 
  {

      val format =
        if (attachment.isDefined) MultiPart
        else if (richMessage.isDefined) Rich
        else Plain

      val commonsMail: Email = format match {
        case Plain => new SimpleEmail().setMsg(message)
        case Rich => new HtmlEmail().setHtmlMsg(richMessage.get).setTextMsg(message)
        case MultiPart => {
          val attachment = new EmailAttachment()
          //attachment.setPath(attachment.get)
          attachment.setDisposition(EmailAttachment.ATTACHMENT)
          attachment.setName(attachment.get.getName)
          new MultiPartEmail().attach(attachment).setMsg(message)
        }
      }

      // Can't add these via fluent API because it produces exceptions
      to foreach (commonsMail.addTo(_))
      cc foreach (commonsMail.addCc(_))
      bcc foreach (commonsMail.addBcc(_))

      commonsMail.
        setFrom(from._1, from._2).
        setSubject(subject).
        send()
    }
  }